package types

import "time"

type Message struct {
	Value     []byte
	Key       []byte
	Topic     string
	Partition int32
	Offset    int64
	Headers   map[string]string
	Timestamp time.Time
}

type Event struct {
	Timestamp time.Time
	Topic     string
	Partition int32
	Offset    int64
	Fields    map[string]interface{}
}

func NewEvent(timestamp time.Time, level string, message string) *Event {
	return &Event{
		Timestamp: timestamp,
	}
}

func (e *Event) AddField(key string, value interface{}) {
	e.Fields[key] = value
}
