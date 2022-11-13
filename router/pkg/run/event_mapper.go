package run

import (
	"fmt"
	"math"

	"github.com/go-mysql-org/go-mysql/canal"
)

type EventMapper struct {
	aggregateIDColumnName string
	payloadColumnName     string
}

type NotInsertActionErr struct {
	actual string
}

func (n *NotInsertActionErr) Error() string {
	return fmt.Sprintf(
		"expected %s action, received: %s",
		canal.InsertAction,
		n.actual,
	)
}

func (e *EventMapper) Map(event *canal.RowsEvent) ([]OutboxEvent, error) {
	if event.Action != canal.InsertAction {
		return nil, &NotInsertActionErr{
			actual: event.Action,
		}
	}

	aggregateIDIndex := -1
	payloadIndex := -1
	for i, column := range event.Table.Columns {
		if column.Name == e.aggregateIDColumnName {
			aggregateIDIndex = i
			continue
		}

		if column.Name == e.payloadColumnName {
			payloadIndex = i
			continue
		}
	}

	if aggregateIDIndex == -1 || payloadIndex == -1 {
		return nil, fmt.Errorf("outbox table miss '%s' or '%s' column", e.aggregateIDColumnName, e.payloadColumnName)
	}

	oes := make([]OutboxEvent, 0, len(event.Rows))
	for _, row := range event.Rows {
		if len(row) <= int(math.Max(float64(aggregateIDIndex), float64(payloadIndex))) {
			return nil, fmt.Errorf("unexpected event row size")
		}

		aggregateID, ok := row[aggregateIDIndex].(string)
		if !ok {
			return nil, fmt.Errorf("aggregate_id is not string")
		}

		payload, ok := row[payloadIndex].([]byte)
		if !ok {
			return nil, fmt.Errorf("payload is not []byte")
		}

		oes = append(oes, OutboxEvent{
			AggregateID: aggregateID,
			Payload:     payload,
		})
	}

	return oes, nil
}
