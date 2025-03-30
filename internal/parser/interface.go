package parser

import (
	"github.com/hujunhj8610/vector_simulation/internal/pkg/types"
)

type Parser interface {
	Parse(data []byte) ([]*types.Event, error)
}
