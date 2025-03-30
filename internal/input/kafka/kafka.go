package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/input"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

// KafkaInput 实现了从Kafka读取消息的输入组件
type KafkaInput struct {
	ctx      context.Context
	cancel   context.CancelFunc
	cfg      *config.InputConfig
	log      logger.Logger
	client   sarama.ConsumerGroup
	topics   []string
	messages []*types.Message // 作为消息缓冲区
	mutex    sync.Mutex
	ready    chan bool // 用于协调消费者组重平衡
	closed   bool
}

// consumer 实现 sarama.ConsumerGroupHandler 接口
type consumer struct {
	input *KafkaInput
}

// NewKafkaReader 创建一个新的 Kafka 读取器
func NewKafkaReader(pipelineCtx context.Context, kafkaCluster *config.KafkaClusterConfig, cfg *config.InputConfig, log logger.Logger) (input.Reader, error) {
	ctx, cancel := context.WithCancel(pipelineCtx)

	log.Info("Creating Kafka reader", "brokers", kafkaCluster.Brokers, "group", cfg.ConsumerGroup)

	// 创建 Sarama 配置
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaConfig.Version = sarama.V2_0_0_0 // 使用较新的 Kafka 协议版本

	// 设置 SASL 认证 (如果配置了)
	if kafkaCluster.SASL != nil && kafkaCluster.SASL.Enabled {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(kafkaCluster.SASL.Mechanism)
		saramaConfig.Net.SASL.User = kafkaCluster.SASL.Username
		saramaConfig.Net.SASL.Password = kafkaCluster.SASL.Password
	}

	// 设置 TLS (如果配置了)
	if kafkaCluster.TLS != nil && kafkaCluster.TLS.Enabled {
		saramaConfig.Net.TLS.Enable = true
		// TODO: 如果需要，在这里配置 TLS 证书
	}

	// 创建消费者组
	client, err := sarama.NewConsumerGroup(kafkaCluster.Brokers, cfg.ConsumerGroup, saramaConfig)
	if err != nil {
		log.Error("Failed to create Kafka consumer group", "error", err)
		cancel()
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	input := &KafkaInput{
		ctx:      ctx,
		cancel:   cancel,
		cfg:      cfg,
		log:      log,
		client:   client,
		topics:   cfg.Topics,
		ready:    make(chan bool),
		messages: make([]*types.Message, 0, 100),
	}

	// 在后台启动消费循环
	go func() {
		handler := &consumer{input: input}
		for {
			// 检查上下文是否已取消
			if input.ctx.Err() != nil {
				return
			}

			// 消费消息，这个调用是阻塞的
			if err := client.Consume(input.ctx, cfg.Topics, handler); err != nil {
				if !input.closed { // 只在非正常关闭时记录错误
					log.Error("Error from consumer", "error", err)
					time.Sleep(1 * time.Second) // 简单的重试策略
				}
			}

			// 检查消费者组是否已就绪
			if input.closed {
				return
			}
		}
	}()

	// 等待消费者组准备就绪
	<-input.ready
	log.Info("Kafka reader is ready to consume messages")

	return input, nil
}

// Read 实现了 input.Reader 接口，返回从 Kafka 读取的消息
func (k *KafkaInput) Read() ([]*types.Message, error) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if len(k.messages) == 0 {
		return []*types.Message{}, nil
	}

	// 克隆消息列表并清空缓冲区
	messages := make([]*types.Message, len(k.messages))
	copy(messages, k.messages)
	k.messages = k.messages[:0]

	return messages, nil
}

// Close 关闭 Kafka 连接并释放资源
func (k *KafkaInput) Close() error {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	if k.closed {
		return nil
	}

	k.log.Info("Closing Kafka reader")
	k.closed = true
	k.cancel()

	if err := k.client.Close(); err != nil {
		k.log.Error("Error closing Kafka consumer group", "error", err)
		return fmt.Errorf("error closing Kafka consumer group: %w", err)
	}

	return nil
}

// 以下是 sarama.ConsumerGroupHandler 接口的实现

// Setup 在消费者会话开始时被调用
func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	c.input.log.Debug("Kafka consumer session setup",
		"member_id", session.MemberID(),
		"generation_id", session.GenerationID())

	// 标记消费者已准备好
	select {
	case c.input.ready <- true:
	default:
	}

	return nil
}

// Cleanup 在消费者会话结束时被调用
func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	c.input.log.Debug("Kafka consumer session cleanup")
	return nil
}

// ConsumeClaim 处理分配给这个消费者的消息
func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// 注意: 每个分区都会调用 ConsumeClaim，可能会并行执行
	c.input.log.Debug("Starting to consume partition",
		"topic", claim.Topic(),
		"partition", claim.Partition(),
		"initial_offset", claim.InitialOffset())

	// 处理分配给这个消费者的消息
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				c.input.log.Debug("Message channel closed",
					"topic", claim.Topic(),
					"partition", claim.Partition())
				return nil
			}

			c.input.mutex.Lock()
			// 创建自定义消息结构
			message := &types.Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
				Headers:   make(map[string]string),
			}

			// 处理消息头
			for _, header := range msg.Headers {
				message.Headers[string(header.Key)] = string(header.Value)
			}

			// 添加到消息缓冲区
			c.input.messages = append(c.input.messages, message)
			c.input.mutex.Unlock()

			// 标记消息为已处理
			session.MarkMessage(msg, "")

			c.input.log.Debug("Consumed message",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset)

		case <-c.input.ctx.Done():
			return nil
		}
	}
}
