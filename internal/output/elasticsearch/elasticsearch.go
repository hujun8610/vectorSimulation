package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/output"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

// ElasticsearchOutput 实现向 Elasticsearch 批量写入数据
type ElasticsearchOutput struct {
	ctx           context.Context
	cancel        context.CancelFunc
	client        *elasticsearch.Client
	log           logger.Logger
	indexPattern  string
	bulkSize      int
	flushInterval time.Duration
	buffer        []*types.Event
	bufferMutex   sync.Mutex
	flushTicker   *time.Ticker
	wg            sync.WaitGroup
	closed        bool
}

// NewElasticsearchOutput 创建新的 Elasticsearch 输出
func NewElasticsearchOutput(pipelineCtx context.Context, esCluster *config.ESClusterConfig, cfg *config.OutputConfig, flushInterval time.Duration, log logger.Logger) (output.Writer, error) {
	ctx, cancel := context.WithCancel(pipelineCtx)

	log.Info("Creating Elasticsearch output", "hosts", strings.Join(esCluster.Hosts, ","))

	// 创建 Elasticsearch 配置
	esConfig := elasticsearch.Config{
		Addresses: esCluster.Hosts,
	}

	// 设置认证
	if esCluster.Username != "" && esCluster.Password != "" {
		esConfig.Username = esCluster.Username
		esConfig.Password = esCluster.Password
	} else if esCluster.APIKey != "" {
		esConfig.APIKey = esCluster.APIKey
	}

	// 设置 TLS 配置
	if esCluster.TLS != nil && esCluster.TLS.Enabled {
		// TODO: 完善 TLS 配置支持
	}

	// 创建 Elasticsearch 客户端
	client, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		log.Error("Failed to create Elasticsearch client", "error", err)
		cancel()
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	// 检查 Elasticsearch 连接是否正常
	res, err := client.Info()
	if err != nil {
		log.Error("Failed to connect to Elasticsearch", "error", err)
		cancel()
		return nil, fmt.Errorf("failed to connect to Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Error("Elasticsearch returned error", "status", res.Status())
		cancel()
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	output := &ElasticsearchOutput{
		ctx:           ctx,
		cancel:        cancel,
		client:        client,
		log:           log,
		indexPattern:  cfg.IndexPattern,
		bulkSize:      cfg.BulkSize,
		flushInterval: flushInterval,
		buffer:        make([]*types.Event, 0, cfg.BulkSize),
	}

	// 启动定时刷新任务
	output.startFlushTimer()

	log.Info("Elasticsearch output created successfully",
		"bulk_size", cfg.BulkSize,
		"flush_interval", flushInterval.String())

	return output, nil
}

// Write 写入事件到缓冲区，当缓冲区达到 bulkSize 时自动刷新
func (e *ElasticsearchOutput) Write(events []*types.Event) error {
	if e.closed {
		return fmt.Errorf("attempt to write to closed Elasticsearch output")
	}

	if len(events) == 0 {
		return nil
	}

	e.bufferMutex.Lock()
	defer e.bufferMutex.Unlock()

	// 添加事件到缓冲区
	e.buffer = append(e.buffer, events...)

	// 如果缓冲区达到或超过 bulkSize，执行批量写入
	if len(e.buffer) >= e.bulkSize {
		if err := e.flush(); err != nil {
			e.log.Error("Failed to flush events to Elasticsearch", "error", err)
			return err
		}
	}

	return nil
}

// 开始定时刷新
func (e *ElasticsearchOutput) startFlushTimer() {
	e.flushTicker = time.NewTicker(e.flushInterval)
	e.wg.Add(1)

	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-e.ctx.Done():
				e.log.Debug("Flush timer received shutdown signal")
				return
			case <-e.flushTicker.C:
				e.bufferMutex.Lock()
				if len(e.buffer) > 0 {
					if err := e.flush(); err != nil {
						e.log.Error("Failed to flush events", "error", err)
					}
				}
				e.bufferMutex.Unlock()
			}
		}
	}()
}

// 执行批量写入 Elasticsearch
func (e *ElasticsearchOutput) flush() error {
	if len(e.buffer) == 0 {
		return nil
	}

	e.log.Debug("Flushing events to Elasticsearch", "count", len(e.buffer))

	// 准备批量请求主体
	var buf bytes.Buffer

	// 记录操作数
	docsCount := 0

	// 构建批量请求主体
	for _, event := range e.buffer {
		// 确定索引名称 (支持使用时间格式)
		indexName := e.formatIndexName(e.indexPattern, event.Timestamp)

		// 创建索引操作请求
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
			},
		}

		// 添加操作元数据
		if err := json.NewEncoder(&buf).Encode(meta); err != nil {
			return fmt.Errorf("encoding index action metadata failed: %w", err)
		}

		// 添加文档内容
		if err := json.NewEncoder(&buf).Encode(event.Fields); err != nil {
			return fmt.Errorf("encoding document failed: %w", err)
		}

		docsCount++
	}

	// 执行批量请求
	req := esapi.BulkRequest{
		Body:    bytes.NewReader(buf.Bytes()),
		Refresh: "false",
	}

	res, err := req.Do(context.Background(), e.client)
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer res.Body.Close()

	// 检查响应是否有错误
	if res.IsError() {
		var raw map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			return fmt.Errorf("bulk request failed with status %s: %w", res.Status(), err)
		}

		return fmt.Errorf("bulk request failed with status %s: %v", res.Status(), raw)
	}

	// 检查响应中是否有错误项
	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("parsing bulk response failed: %w", err)
	}

	// 检查是否有错误
	if hasErrors, ok := bulkResponse["errors"].(bool); ok && hasErrors {
		// 在实际应用中，可能需要更详细地处理每个文档的错误
		e.log.Warn("Some items in the bulk request failed", "response", bulkResponse)
	}

	// 记录成功信息
	e.log.Info("Successfully flushed events to Elasticsearch",
		"count", docsCount,
		"took_ms", bulkResponse["took"])

	// 清空缓冲区
	e.buffer = e.buffer[:0]

	return nil
}

// 格式化索引名称，支持时间变量
func (e *ElasticsearchOutput) formatIndexName(pattern string, timestamp time.Time) string {
	// 替换 %{+yyyy.MM.dd} 这样的格式
	result := pattern

	// 查找所有 %{+...} 格式的时间变量
	if strings.Contains(pattern, "%{+") {
		// 简单实现，仅处理常见的格式
		result = strings.ReplaceAll(result, "%{+yyyy}", timestamp.Format("2006"))
		result = strings.ReplaceAll(result, "%{+MM}", timestamp.Format("01"))
		result = strings.ReplaceAll(result, "%{+dd}", timestamp.Format("02"))
		result = strings.ReplaceAll(result, "%{+yyyy.MM.dd}", timestamp.Format("2006.01.02"))
		result = strings.ReplaceAll(result, "%{+yyyy-MM-dd}", timestamp.Format("2006-01-02"))
	}

	return result
}

// Close 关闭 Elasticsearch 输出，刷新剩余数据并释放资源
func (e *ElasticsearchOutput) Close() error {
	e.bufferMutex.Lock()
	defer e.bufferMutex.Unlock()

	if e.closed {
		return nil
	}

	e.log.Info("Closing Elasticsearch output, flushing remaining events", "count", len(e.buffer))

	e.closed = true

	// 停止定时器
	if e.flushTicker != nil {
		e.flushTicker.Stop()
	}

	// 取消上下文
	e.cancel()

	// 等待定时器 goroutine 退出
	e.wg.Wait()

	// 刷新剩余的数据
	if len(e.buffer) > 0 {
		if err := e.flush(); err != nil {
			e.log.Error("Failed to flush remaining events during shutdown", "error", err)
			return err
		}
	}

	e.log.Info("Elasticsearch output closed successfully")
	return nil
}
