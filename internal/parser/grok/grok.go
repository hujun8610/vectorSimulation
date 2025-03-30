package grok

import (
	"fmt"
	"time"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/parser"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
	"github.com/vjeantet/grok"
)

// GrokParser 实现了使用 Grok 模式解析日志的解析器
type GrokParser struct {
	pattern    string
	grokParser *grok.Grok
	timeField  string
	timeFormat string
	addFields  map[string]interface{}
}

// NewGrokParser 创建一个新的 Grok 解析器
func NewGrokParser(cfg *config.ParserConfig) (parser.Parser, error) {
	if cfg.Pattern == "" {
		return nil, fmt.Errorf("grok parser requires a pattern")
	}

	// 创建 grok 实例，自动加载默认模式
	grokParser, err := grok.NewWithConfig(&grok.Config{
		NamedCapturesOnly: true, // 只捕获命名组
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create grok parser: %w", err)
	}

	// 添加自定义模式
	customPatterns := map[string]string{
		"LOGLEVEL":     "([A-a]lert|ALERT|[T|t]race|TRACE|[D|d]ebug|DEBUG|[N|n]otice|NOTICE|[I|i]nfo|INFO|[W|w]arn(?:ing)?|WARN(?:ING)?|[E|e]rr(?:or)?|ERR(?:OR)?|[C|c]rit(?:ical)?|CRIT(?:ICAL)?|[F|f]atal|FATAL|[S|s]evere|SEVERE|EMERGENCY|[E|e]mergency)",
		"K8S_LOG":      "%{TIMESTAMP_ISO8601:timestamp}\\s+%{LOGLEVEL:level}\\s+%{GREEDYDATA:message}",
		"APP_LOG":      "%{TIMESTAMP_ISO8601:timestamp}\\s+\\[%{LOGLEVEL:level}\\]\\s+%{NOTSPACE:logger}\\s+-\\s+%{GREEDYDATA:message}",
		"NGINX_ACCESS": "%{IPORHOST:clientip} - %{NOTSPACE:remote_user} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:method} %{NOTSPACE:request} HTTP/%{NUMBER:http_version}\" %{NUMBER:status} %{NUMBER:body_bytes_sent} \"%{NOTSPACE:http_referer}\" \"%{NOTSPACE:http_user_agent}\" %{NUMBER:request_time} %{NUMBER:upstream_response_time}",
	}

	for name, pattern := range customPatterns {
		if err := grokParser.AddPattern(name, pattern); err != nil {
			return nil, fmt.Errorf("failed to add custom pattern %s: %w", name, err)
		}
	}

	// 检查是否是使用预定义模式语法: %{PREDEFINED:名称}
	pattern := cfg.Pattern
	if len(pattern) > 2 && pattern[0] == '%' && pattern[1] == '{' {
		endBrace := -1
		for i := 2; i < len(pattern); i++ {
			if pattern[i] == '}' {
				endBrace = i
				break
			}
		}

		if endBrace > 0 {
			predefinedName := pattern[2:endBrace]
			// 检查是否存在预定义模式
			if predefinedPattern, exists := PredefinedPatterns[predefinedName]; exists {
				pattern = predefinedPattern
			}
		}
	}

	// 设置时间字段和格式，或使用默认值
	timeField := cfg.TimeField
	if timeField == "" {
		timeField = "@timestamp"
	}

	timeFormat := cfg.TimeFormat
	if timeFormat == "" {
		timeFormat = time.RFC3339
	}

	return &GrokParser{
		pattern:    pattern, // 使用可能被替换的模式
		grokParser: grokParser,
		timeField:  timeField,
		timeFormat: timeFormat,
		addFields:  cfg.AddFields,
	}, nil
}

// Parse 解析输入的日志数据
func (p *GrokParser) Parse(data []byte) ([]*types.Event, error) {
	if len(data) == 0 {
		return []*types.Event{}, nil
	}

	// 应用 grok 模式解析数据
	matches, err := p.grokParser.Parse(p.pattern, string(data))
	if err != nil {
		return nil, fmt.Errorf("grok parsing failed: %w", err)
	}

	// 如果模式不匹配，返回错误
	if len(matches) == 0 {
		return nil, fmt.Errorf("input does not match grok pattern: %s", p.pattern)
	}

	// 创建事件
	event := types.NewEvent(time.Now())

	// 添加所有匹配的字段
	for k, v := range matches {
		event.AddField(k, v)
	}

	// 处理时间字段，如果存在
	if timeStr, ok := matches[p.timeField]; ok {
		// 尝试按配置的格式解析时间
		if parsedTime, err := time.Parse(p.timeFormat, timeStr); err == nil {
			// 使用解析后的时间替换原始字符串
			event.AddField(p.timeField, parsedTime.Format(time.RFC3339))
		}
	} else {
		// 如果日志中没有时间字段，添加当前时间
		event.AddField(p.timeField, time.Now().Format(time.RFC3339))
	}

	// 添加额外配置的字段
	for k, v := range p.addFields {
		event.AddField(k, v)
	}

	// 返回事件数组（只有一个元素）
	return []*types.Event{event}, nil
}

// 预定义的常用 Grok 模式
var PredefinedPatterns = map[string]string{
	"Apache Common":   "%{COMMONAPACHELOG}",
	"Apache Combined": "%{COMBINEDAPACHELOG}",
	"Apache Error":    "\\[%{HTTPDATE:timestamp}\\] \\[%{LOGLEVEL:level}\\] \\[client %{IPORHOST:clientip}\\] %{GREEDYDATA:message}",
	"Nginx Access":    "%{NGINX_ACCESS}",
	"Syslog RFC5424":  "%{SYSLOG5424PRI}%{NONNEGINT:syslog_ver} +(?:%{TIMESTAMP_ISO8601:timestamp}|-) +(?:%{HOSTNAME:syslog_host}|-) +(?:%{WORD:syslog_app}|-) +(?:%{WORD:syslog_proc}|-) +(?:%{WORD:syslog_msgid}|-) +(?:%{SYSLOG5424SD:syslog_sd}|-|\\[%{DATA:syslog_sd}\\]) +%{GREEDYDATA:message}",
	"Syslog RFC3164":  "%{SYSLOGBASE} %{GREEDYDATA:message}",
	"JSON Log":        "(?:%{TIMESTAMP_ISO8601:timestamp})?\\s*%{GREEDYDATA:json}",
	"Kubernetes Log":  "%{K8S_LOG}",
	"Application Log": "%{APP_LOG}",
}

// GetPredefinedPattern 返回预定义的 Grok 模式
func GetPredefinedPattern(name string) string {
	if pattern, ok := PredefinedPatterns[name]; ok {
		return pattern
	}
	return ""
}
