package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/pipeline"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
)

type Engine struct {
	ctx       context.Context
	cfg       *config.Config
	log       logger.Logger
	pipelines []*pipeline.Pipeline
	wg        sync.WaitGroup
}

// NewEngine 创建新的引擎实例
func NewEngine(ctx context.Context, cfg *config.Config, log logger.Logger) (*Engine, error) {
	return &Engine{
		ctx:       ctx,
		cfg:       cfg,
		log:       log,
		pipelines: []*pipeline.Pipeline{},
	}, nil
}

// Start 启动引擎和所有管道
func (e *Engine) Start() error {
	e.log.Info("Starting engine...")

	// 创建并启动每个管道
	for _, pCfg := range e.cfg.Pipelines {
		if !pCfg.Enabled {
			e.log.Info("Skipping disabled pipeline", "pipeline", pCfg.Name)
			continue
		}

		pl, err := pipeline.NewPipeline(e.ctx, pCfg, e.cfg, e.log)
		if err != nil {
			return fmt.Errorf("failed to create pipeline %s: %w", pCfg.Name, err)
		}

		e.pipelines = append(e.pipelines, pl)
		e.wg.Add(1)

		go func(pl *pipeline.Pipeline) {
			defer e.wg.Done()
			if err := pl.Start(); err != nil {
				e.log.Error("Pipeline error", "pipeline", pl.Name(), "error", err)
			}
		}(pl)

		e.log.Info("Started pipeline", "pipeline", pCfg.Name)
	}

	e.log.Info("Engine started successfully with", "pipelines", len(e.pipelines))
	return nil
}

// Stop 优雅地停止引擎和所有管道
func (e *Engine) Stop(ctx context.Context) error {
	e.log.Info("Stopping engine...")

	// 停止所有管道
	var firstErr error
	for _, p := range e.pipelines {
		if err := p.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
			e.log.Error("Error stopping pipeline", "pipeline", p.Name(), "error", err)
		}
	}

	// 等待所有管道退出
	waitCh := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		e.log.Info("All pipelines stopped successfully")
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for pipelines to stop: %w", ctx.Err())
	}

	return firstErr
}
