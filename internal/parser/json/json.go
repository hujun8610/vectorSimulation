package json

import (
	"github.com/hujunhj8610/vector_simulation/internal/config"
	"github.com/hujunhj8610/vector_simulation/internal/parser"
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

type JSONParser struct {
	parser.Parser
}

func NewJSONParser(cfg *config.ParserConfig) (parser.Parser, error) {
	return nil, nil
}

func (p *JSONParser) Parse(data []byte) ([]*types.Event, error) {
	return nil, nil
}
