package kafka

import (
	"context"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/input"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

type KafkaInput struct {
	input.Reader
}

//	reader, err := input.NewKafkaReader(pipelineCtx, kafkaCluster, &cfg.Input, log)

func NewKafkaReader(pipelineCtx context.Context, kafkaCluster *config.KafkaClusterConfig, cfg *config.InputConfig, log logger.Logger) (input.Reader, error) {
	return nil, nil
}

func (k *KafkaInput) Read() ([]*types.Message, error) {
	return nil, nil
}

func (k *KafkaInput) Close() error {
	return nil
}
