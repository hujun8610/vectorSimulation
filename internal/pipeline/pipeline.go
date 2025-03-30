package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/input"
	"github.com/hujunhj8610/vector_simulation/internal/input/kafka"
	"github.com/hujunhj8610/vector_simulation/internal/output"
	"github.com/hujunhj8610/vector_simulation/internal/output/elasticsearch"
	"github.com/hujunhj8610/vector_simulation/internal/parser"
	"github.com/hujunhj8610/vector_simulation/internal/parser/grok"
	"github.com/hujunhj8610/vector_simulation/internal/parser/json"
	"github.com/hujunhj8610/vector_simulation/internal/parser/regex"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

// Pipeline 代表一条处理管道
type Pipeline struct {
	ctx    context.Context
	cancel context.CancelFunc
	cfg    config.PipelineConfig
	log    logger.Logger
	name   string

	// 组件
	reader input.Reader
	parser parser.Parser
	writer output.Writer

	// 通道
	rawMsgCh    chan *types.Message
	parsedMsgCh chan *types.Event

	// 同步
	wg sync.WaitGroup
}

// NewPipeline 创建新的管道
func NewPipeline(
	ctx context.Context,
	cfg config.PipelineConfig,
	globalCfg *config.Config,
	log logger.Logger,
) (*Pipeline, error) {
	pipelineCtx, cancel := context.WithCancel(ctx)

	p := &Pipeline{
		ctx:         pipelineCtx,
		cancel:      cancel,
		cfg:         cfg,
		log:         log.With("pipeline", cfg.Name),
		name:        cfg.Name,
		rawMsgCh:    make(chan *types.Message, 1000), // 可配置的缓冲区大小
		parsedMsgCh: make(chan *types.Event, 1000),   // 可配置的缓冲区大小
	}

	// 查找Kafka集群配置
	var kafkaCluster *config.KafkaClusterConfig
	for _, k := range globalCfg.KafkaClusters {
		if k.Name == cfg.Input.ClusterName {
			kafkaCluster = &k
			break
		}
	}
	if kafkaCluster == nil {
		return nil, fmt.Errorf("kafka cluster %s not found", cfg.Input.ClusterName)
	}

	// 查找ES集群配置
	var esCluster *config.ESClusterConfig
	for _, e := range globalCfg.ESClusters {
		if e.Name == cfg.Output.ClusterName {
			esCluster = &e
			break
		}
	}
	if esCluster == nil {
		return nil, fmt.Errorf("elasticsearch cluster %s not found", cfg.Output.ClusterName)
	}

	// 创建输入组件 (Kafka reader)
	reader, err := kafka.NewKafkaReader(pipelineCtx, kafkaCluster, &cfg.Input, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka reader: %w", err)
	}
	p.reader = reader

	// 创建解析器
	var parser parser.Parser
	switch cfg.Parser.Type {
	case "regex":
		parser, err = grok.NewGrokParser(&cfg.Parser)
	case "json":
		parser, err = json.NewJSONParser(&cfg.Parser)
	case "grok":
		parser, err = regex.NewRegexParser(&cfg.Parser)
	default:
		return nil, fmt.Errorf("unsupported parser type: %s", cfg.Parser.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}
	p.parser = parser

	// 创建输出组件 (ES writer)
	flushInterval, err := time.ParseDuration(cfg.Output.FlushInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid flush interval: %w", err)
	}

	writer, err := elasticsearch.NewElasticsearchOutput(pipelineCtx, esCluster, &cfg.Output, flushInterval, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch writer: %w", err)
	}
	p.writer = writer

	return p, nil
}

// Name 返回管道名称
func (p *Pipeline) Name() string {
	return p.name
}

// Start 启动管道处理
func (p *Pipeline) Start() error {
	p.log.Info("Starting pipeline...")

	// 启动输入阶段
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.startReader()
	}()

	// 启动解析阶段 (多个工作者)
	numParsers := 4 // 可以从配置中获取
	for i := 0; i < numParsers; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			p.startParser(workerID)
		}(i)
	}

	// 启动输出阶段
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.startWriter()
	}()

	p.log.Info("Pipeline started successfully")
	return nil
}

// startReader 启动读取Kafka数据的goroutine
func (p *Pipeline) startReader() {
	p.log.Info("Starting reader...")

	for {
		select {
		case <-p.ctx.Done():
			p.log.Info("Reader received shutdown signal")
			return
		default:
			// 从Kafka读取消息
			msgCh, err := p.reader.Read()
			if err != nil {
				p.log.Error("Error reading messages", "error", err)
				// 简单重试策略
				time.Sleep(1 * time.Second)
				continue
			}

			// 处理接收到的消息
			for _, msg := range msgCh {
				select {
				case p.rawMsgCh <- msg:
					// 消息已放入通道
				case <-p.ctx.Done():
					p.log.Info("Reader shutting down while processing messages")
					return
				}
			}
		}
	}
}

// startParser 启动解析日志的goroutine
func (p *Pipeline) startParser(workerID int) {
	p.log.Info("Starting parser worker", "worker_id", workerID)

	for {
		select {
		case <-p.ctx.Done():
			p.log.Info("Parser received shutdown signal", "worker_id", workerID)
			return
		case msg := <-p.rawMsgCh:
			// 解析消息
			event, err := p.parser.Parse(msg.Value)
			if err != nil {
				p.log.Error("Error parsing message", "error", err, "worker_id", workerID)
				// 处理解析失败的情况
				// 可以选择记录或发送到死信队列
				continue
			}

			// 添加额外字段和元数据
			for _, e := range event {
				e.AddField("@timestamp", time.Now().Format(time.RFC3339))
				for k, v := range p.cfg.Parser.AddFields {
					e.AddField(k, v)
				}
				e.AddField("kafka_topic", msg.Topic)
				e.AddField("kafka_partition", msg.Partition)
				e.AddField("kafka_offset", msg.Offset)

				// 发送到输出通道
				select {
				case p.parsedMsgCh <- e:
					// 消息已放入通道
				case <-p.ctx.Done():
					p.log.Info("Parser shutting down while sending parsed message", "worker_id", workerID)
					return
				}
			}
		}
	}
}

// startWriter 启动写入ES的goroutine
func (p *Pipeline) startWriter() {
	p.log.Info("Starting writer...")

	for {
		select {
		case <-p.ctx.Done():
			p.log.Info("Writer received shutdown signal")
			return
		case event := <-p.parsedMsgCh:
			if err := p.writer.Write([]*types.Event{event}); err != nil {
				p.log.Error("Error writing event to Elasticsearch", "error", err)
				// 处理写入失败的情况
			}
		}
	}
}

// Stop 停止管道及其组件
func (p *Pipeline) Stop(ctx context.Context) error {
	p.log.Info("Stopping pipeline...")

	// 取消管道上下文，通知所有goroutine停止
	p.cancel()

	// 关闭输入读取器
	if err := p.reader.Close(); err != nil {
		p.log.Error("Error closing reader", "error", err)
	}

	// 等待所有goroutine完成
	waitCh := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		p.log.Info("All pipeline goroutines stopped")
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for pipeline goroutines to stop")
	}

	// 确保Writer刷新并关闭
	if err := p.writer.Close(); err != nil {
		p.log.Error("Error closing writer", "error", err)
		return err
	}

	p.log.Info("Pipeline stopped successfully")
	return nil
}
