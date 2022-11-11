package string

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
)

type Serializer struct {
}

func (j *Serializer) SerializeKey(e *canal.RowsEvent) (interface{}, error) {
	return "", nil
}

func (j *Serializer) SerializeMessage(e *canal.RowsEvent) (interface{}, error) {
	return fmt.Sprintf("%v", e.Rows), nil
}
