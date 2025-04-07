package json

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/parser"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

// JSONParser 实现了使用JSON解析日志的解析器
type JSONParser struct {
	timeField  string
	timeFormat string
	addFields  map[string]interface{}
}

// NewJSONParser 创建一个新的JSON解析器
func NewJSONParser(cfg *config.ParserConfig) (parser.Parser, error) {
	// 设置时间字段和格式，或使用默认值
	timeField := cfg.TimeField
	if timeField == "" {
		timeField = "@timestamp"
	}

	timeFormat := cfg.TimeFormat
	if timeFormat == "" {
		timeFormat = time.RFC3339
	}

	return &JSONParser{
		timeField:  timeField,
		timeFormat: timeFormat,
		addFields:  cfg.AddFields,
	}, nil
}

// Parse 使用JSON解析输入的日志数据
func (p *JSONParser) Parse(data []byte) ([]*types.Event, error) {
	if len(data) == 0 {
		return []*types.Event{}, nil
	}

	// 解析JSON数据
	var fields map[string]interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// 创建事件
	event := types.NewEvent(time.Now())

	// 添加所有字段
	for k, v := range fields {
		event.AddField(k, v)
	}

	// 处理时间字段，如果存在
	if timeStr, ok := fields[p.timeField].(string); ok {
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
