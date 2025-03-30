package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	// 处理环境变量覆盖
	if err := processEnvOverrides(&cfg); err != nil {
		return nil, fmt.Errorf("error processing environment variables: %w", err)
	}

	// 验证配置
	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// processEnvOverrides 处理环境变量覆盖配置
func processEnvOverrides(cfg *Config) error {
	// 实现环境变量到配置的映射
	// 例如 LOG_PROCESSOR_LOGGING_LEVEL 环境变量会覆盖 cfg.Logging.Level
	// ...
	return nil
}

// validateConfig 验证配置的有效性
func validateConfig(cfg *Config) error {
	// 验证Kafka集群配置
	kafkaClusters := make(map[string]bool)
	for _, k := range cfg.KafkaClusters {
		if k.Name == "" {
			return fmt.Errorf("kafka cluster name cannot be empty")
		}
		if len(k.Brokers) == 0 {
			return fmt.Errorf("kafka cluster %s has no brokers", k.Name)
		}
		kafkaClusters[k.Name] = true
	}

	// 验证ES集群配置
	esClusters := make(map[string]bool)
	for _, e := range cfg.ESClusters {
		if e.Name == "" {
			return fmt.Errorf("elasticsearch cluster name cannot be empty")
		}
		if len(e.Hosts) == 0 {
			return fmt.Errorf("elasticsearch cluster %s has no hosts", e.Name)
		}
		esClusters[e.Name] = true
	}

	// 验证Pipeline配置
	for i, p := range cfg.Pipelines {
		if p.Name == "" {
			return fmt.Errorf("pipeline #%d has no name", i+1)
		}

		// 检查输入配置
		if p.Input.Type != "kafka" {
			return fmt.Errorf("pipeline %s has unsupported input type: %s", p.Name, p.Input.Type)
		}
		if !kafkaClusters[p.Input.ClusterName] {
			return fmt.Errorf("pipeline %s references unknown kafka cluster: %s", p.Name, p.Input.ClusterName)
		}
		if len(p.Input.Topics) == 0 {
			return fmt.Errorf("pipeline %s has no kafka topics", p.Name)
		}
		if p.Input.ConsumerGroup == "" {
			return fmt.Errorf("pipeline %s has no consumer group", p.Name)
		}

		// 检查解析配置
		switch p.Parser.Type {
		case "regex", "grok":
			if p.Parser.Pattern == "" {
				return fmt.Errorf("pipeline %s has %s parser but no pattern", p.Name, p.Parser.Type)
			}
		case "json":
			// JSON解析器不需要pattern
		default:
			return fmt.Errorf("pipeline %s has unsupported parser type: %s", p.Name, p.Parser.Type)
		}

		// 检查输出配置
		if p.Output.Type != "elasticsearch" {
			return fmt.Errorf("pipeline %s has unsupported output type: %s", p.Name, p.Output.Type)
		}
		if !esClusters[p.Output.ClusterName] {
			return fmt.Errorf("pipeline %s references unknown elasticsearch cluster: %s", p.Name, p.Output.ClusterName)
		}
		if p.Output.IndexPattern == "" {
			return fmt.Errorf("pipeline %s has no elasticsearch index pattern", p.Name)
		}
		if p.Output.BulkSize <= 0 {
			return fmt.Errorf("pipeline %s has invalid bulk size: %d", p.Name, p.Output.BulkSize)
		}
	}

	return nil
}
