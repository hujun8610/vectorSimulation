package regex

import (
	"fmt"
	"regexp"
	"time"

	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/parser"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

// RegexParser 实现了使用正则表达式解析日志的解析器
type RegexParser struct {
	pattern     string
	re          *regexp.Regexp
	subexpNames []string
	timeField   string
	timeFormat  string
	addFields   map[string]interface{}
}

// NewRegexParser 创建一个新的正则表达式解析器
func NewRegexParser(cfg *config.ParserConfig) (parser.Parser, error) {
	if cfg.Pattern == "" {
		return nil, fmt.Errorf("regex parser requires a pattern")
	}

	// 编译正则表达式
	re, err := regexp.Compile(cfg.Pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to compile regex pattern: %w", err)
	}

	// 提取命名捕获组的名称
	subexpNames := re.SubexpNames()

	// 设置时间字段和格式，或使用默认值
	timeField := cfg.TimeField
	if timeField == "" {
		timeField = "@timestamp"
	}

	timeFormat := cfg.TimeFormat
	if timeFormat == "" {
		timeFormat = time.RFC3339
	}

	return &RegexParser{
		pattern:     cfg.Pattern,
		re:          re,
		subexpNames: subexpNames,
		timeField:   timeField,
		timeFormat:  timeFormat,
		addFields:   cfg.AddFields,
	}, nil
}

// Parse 使用正则表达式解析输入的日志数据
func (p *RegexParser) Parse(data []byte) ([]*types.Event, error) {
	if len(data) == 0 {
		return []*types.Event{}, nil
	}

	// 应用正则表达式解析数据
	matches := p.re.FindSubmatch(data)
	if len(matches) == 0 {
		return nil, fmt.Errorf("input does not match regex pattern: %s", p.pattern)
	}

	// 创建事件
	event := types.NewEvent(time.Now())

	// 添加所有匹配的字段
	for i, name := range p.subexpNames {
		if i != 0 && name != "" { // 跳过整个匹配和未命名的捕获组
			value := string(matches[i])
			event.AddField(name, value)

			// 处理时间字段，如果存在
			if name == p.timeField {
				// 尝试按配置的格式解析时间
				if parsedTime, err := time.Parse(p.timeFormat, value); err == nil {
					// 使用解析后的时间替换原始字符串
					event.AddField(p.timeField, parsedTime.Format(time.RFC3339))
				}
			}
		}
	}

	// 如果日志中没有时间字段，添加当前时间
	if _, exists := event.Fields[p.timeField]; !exists {
		event.AddField(p.timeField, time.Now().Format(time.RFC3339))
	}

	// 添加额外配置的字段
	for k, v := range p.addFields {
		event.AddField(k, v)
	}

	// 返回事件数组（只有一个元素）
	return []*types.Event{event}, nil
}
