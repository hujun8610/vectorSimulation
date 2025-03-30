package elasticsearch

import (
	"context"
	"time"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/output"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

type ElasticsearchOutput struct {
	output.Writer
}

// writer, err := output.NewESWriter(pipelineCtx, esCluster, &cfg.Output, flushInterval, log)
func NewElasticsearchOutput(pipelineCtx context.Context, esCluster *config.ESClusterConfig, cfg *config.OutputConfig, flushInterval time.Duration, log logger.Logger) (output.Writer, error) {
	return nil, nil
}

func (e *ElasticsearchOutput) Write(events []*types.Event) error {
	return nil
}

func (e *ElasticsearchOutput) Close() error {
	return nil
}
