package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/engine"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
)

func main() {
	// 解析命令行参数
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// 初始化日志
	log := logger.NewLogger("info", "json")
	log.Info("Starting log processor...")

	// 加载配置
	cfg, err := config.LoadConfig(*configPath, log)
	if err != nil {
		log.Fatal("Failed to load configuration", "error", err)
	}

	// 创建引擎上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建并启动引擎
	eng, err := engine.NewEngine(ctx, cfg, log)
	if err != nil {
		log.Fatal("Failed to create engine", "error", err)
	}

	if err := eng.Start(); err != nil {
		log.Fatal("Failed to start engine", "error", err)
	}

	// 优雅退出处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Info("Received shutdown signal, gracefully shutting down...")

	// 创建一个有超时的上下文
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// 停止引擎
	if err := eng.Stop(shutdownCtx); err != nil {
		log.Error("Error during shutdown", "error", err)
		os.Exit(1)
	}

	log.Info("Log processor shutdown completed")
}
