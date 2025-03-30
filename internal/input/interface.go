package input

import "github.com/hujunhj8610/vector_simulation/internal/pkg/types"

type Reader interface {
	Read() ([]*types.Message, error)
	Close() error
}
