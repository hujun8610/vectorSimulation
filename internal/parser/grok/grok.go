package grok

import (
	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/parser"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

type GrokParser struct {
	parser.Parser
}

func NewGrokParser(cfg *config.ParserConfig) (parser.Parser, error) {
	return nil, nil
}

func (p *GrokParser) Parse(data []byte) ([]*types.Event, error) {
	return nil, nil
}
