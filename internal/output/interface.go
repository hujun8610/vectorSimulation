package output

import "github.com/hujunhj8610/vector_simulation/internal/pkg/types"

type Writer interface {
	Write(events []*types.Event) error
	Close() error
}
