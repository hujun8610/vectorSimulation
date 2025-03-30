package config

// Config 代表整个应用的配置
type Config struct {
	Logging       LoggingConfig        `yaml:"logging"`
	KafkaClusters []KafkaClusterConfig `yaml:"kafka_clusters"`
	ESClusters    []ESClusterConfig    `yaml:"es_clusters"`
	Pipelines     []PipelineConfig     `yaml:"pipelines"`
}

// LoggingConfig 定义日志配置
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// KafkaClusterConfig 定义Kafka集群配置
type KafkaClusterConfig struct {
	Name    string      `yaml:"name"`
	Brokers []string    `yaml:"brokers"`
	SASL    *SASLConfig `yaml:"sasl,omitempty"`
	TLS     *TLSConfig  `yaml:"tls,omitempty"`
}

// SASLConfig 定义Kafka SASL认证配置
type SASLConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// TLSConfig 定义TLS配置
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CACertFile string `yaml:"ca_cert_file,omitempty"`
	CertFile   string `yaml:"cert_file,omitempty"`
	KeyFile    string `yaml:"key_file,omitempty"`
	SkipVerify bool   `yaml:"skip_verify"`
}

// ESClusterConfig 定义ES集群配置
type ESClusterConfig struct {
	Name     string     `yaml:"name"`
	Hosts    []string   `yaml:"hosts"`
	Username string     `yaml:"username,omitempty"`
	Password string     `yaml:"password,omitempty"`
	APIKey   string     `yaml:"api_key,omitempty"`
	TLS      *TLSConfig `yaml:"tls,omitempty"`
}

// PipelineConfig 定义Pipeline配置
type PipelineConfig struct {
	Name    string       `yaml:"name"`
	Enabled bool         `yaml:"enabled"`
	Input   InputConfig  `yaml:"input"`
	Parser  ParserConfig `yaml:"parser"`
	Output  OutputConfig `yaml:"output"`
}

// InputConfig 定义输入源配置
type InputConfig struct {
	Type           string   `yaml:"type"`
	ClusterName    string   `yaml:"cluster"`
	Topics         []string `yaml:"topics"`
	ConsumerGroup  string   `yaml:"consumer_group"`
	CommitInterval string   `yaml:"commit_interval,omitempty"`
}

// ParserConfig 定义解析器配置
type ParserConfig struct {
	Type       string                 `yaml:"type"`
	Pattern    string                 `yaml:"pattern,omitempty"`
	TimeField  string                 `yaml:"time_field,omitempty"`
	TimeFormat string                 `yaml:"time_format,omitempty"`
	AddFields  map[string]interface{} `yaml:"add_fields,omitempty"`
}

// OutputConfig 定义输出目标配置
type OutputConfig struct {
	Type          string `yaml:"type"`
	ClusterName   string `yaml:"cluster"`
	IndexPattern  string `yaml:"index_pattern"`
	BulkSize      int    `yaml:"bulk_size"`
	FlushInterval string `yaml:"flush_interval"`
}
