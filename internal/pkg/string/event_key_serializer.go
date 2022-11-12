package string

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

type EventKeySerializer struct {
}

func (j *EventKeySerializer) SerializeKey(e *canal.RowsEvent) (interface{}, error) {
	return "", nil
}
