package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/hujunhj8610/vector_simulation/internal/pkg/logger"
	"gopkg.in/yaml.v3"
)

// LoadConfig 从文件加载配置
func LoadConfig(path string, log logger.Logger) (*Config, error) {
	log.Info("Loading configuration file", "path", path)

	data, err := os.ReadFile(path)
	if err != nil {
		log.Error("Failed to read config file", "path", path, "error", err)
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Error("Failed to parse config file", "path", path, "error", err)
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	// 处理环境变量覆盖
	if err := processEnvOverrides(&cfg, log); err != nil {
		log.Error("Failed to process environment variables", "error", err)
		return nil, fmt.Errorf("error processing environment variables: %w", err)
	}

	// 验证配置
	if err := validateConfig(&cfg, log); err != nil {
		log.Error("Configuration validation failed", "error", err)
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// 设置默认值
	setDefaultValues(&cfg, log)

	log.Info("Configuration loaded successfully",
		"kafka_clusters", len(cfg.KafkaClusters),
		"es_clusters", len(cfg.ESClusters),
		"pipelines", len(cfg.Pipelines))

	return &cfg, nil
}

// processEnvOverrides 处理环境变量覆盖配置
func processEnvOverrides(cfg *Config, log logger.Logger) error {
	log.Debug("Processing environment variables")

	// 处理日志配置环境变量
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		log.Debug("Overriding log level from environment", "level", level)
		cfg.Logging.Level = level
	}

	if format := os.Getenv("LOG_FORMAT"); format != "" {
		log.Debug("Overriding log format from environment", "format", format)
		cfg.Logging.Format = format
	}

	// 处理Kafka环境变量
	for i := range cfg.KafkaClusters {
		cluster := &cfg.KafkaClusters[i]
		envPrefix := "KAFKA_" + strings.ToUpper(cluster.Name) + "_"

		if brokers := os.Getenv(envPrefix + "BROKERS"); brokers != "" {
			log.Debug("Overriding Kafka brokers from environment",
				"cluster", cluster.Name,
				"brokers", brokers)
			cluster.Brokers = strings.Split(brokers, ",")
		}

		if username := os.Getenv(envPrefix + "USERNAME"); username != "" && cluster.SASL != nil {
			log.Debug("Overriding Kafka SASL username from environment", "cluster", cluster.Name)
			cluster.SASL.Username = username
		}

		if password := os.Getenv(envPrefix + "PASSWORD"); password != "" && cluster.SASL != nil {
			log.Debug("Overriding Kafka SASL password from environment", "cluster", cluster.Name)
			cluster.SASL.Password = password
		}
	}

	// 处理ES环境变量
	for i := range cfg.ESClusters {
		cluster := &cfg.ESClusters[i]
		envPrefix := "ES_" + strings.ToUpper(cluster.Name) + "_"

		if hosts := os.Getenv(envPrefix + "HOSTS"); hosts != "" {
			log.Debug("Overriding ES hosts from environment",
				"cluster", cluster.Name,
				"hosts", hosts)
			cluster.Hosts = strings.Split(hosts, ",")
		}

		if username := os.Getenv(envPrefix + "USERNAME"); username != "" {
			log.Debug("Overriding ES username from environment", "cluster", cluster.Name)
			cluster.Username = username
		}

		if password := os.Getenv(envPrefix + "PASSWORD"); password != "" {
			log.Debug("Overriding ES password from environment", "cluster", cluster.Name)
			cluster.Password = password
		}
	}

	return nil
}

// setDefaultValues 为配置中未设置的字段设置默认值
func setDefaultValues(cfg *Config, log logger.Logger) {
	// 设置日志默认值
	if cfg.Logging.Level == "" {
		log.Debug("Setting default log level", "level", "info")
		cfg.Logging.Level = "info"
	}

	if cfg.Logging.Format == "" {
		log.Debug("Setting default log format", "format", "console")
		cfg.Logging.Format = "console"
	}

	// 设置每个Pipeline的默认值
	for i := range cfg.Pipelines {
		p := &cfg.Pipelines[i]

		// 设置Parser默认值
		if p.Parser.TimeField == "" {
			p.Parser.TimeField = "@timestamp"
		}

		if p.Parser.TimeFormat == "" {
			p.Parser.TimeFormat = "2006-01-02T15:04:05Z07:00" // RFC3339
		}

		// 设置输出默认值
		if p.Output.BulkSize <= 0 {
			p.Output.BulkSize = 1000
		}

		if p.Output.FlushInterval == "" {
			p.Output.FlushInterval = "5s"
		}

		log.Debug("Applied default values to pipeline",
			"pipeline", p.Name,
			"bulk_size", p.Output.BulkSize,
			"flush_interval", p.Output.FlushInterval)
	}
}

// validateConfig 验证配置的有效性
func validateConfig(cfg *Config, log logger.Logger) error {
	log.Debug("Validating configuration")

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
	}

	log.Debug("Configuration validation completed successfully")
	return nil
}
