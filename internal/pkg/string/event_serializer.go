package string

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
)

type EventSerializer struct {
}

func (j *EventSerializer) SerializeMessage(e *canal.RowsEvent) (interface{}, error) {
	return fmt.Sprintf("%v", e.Rows), nil
}
