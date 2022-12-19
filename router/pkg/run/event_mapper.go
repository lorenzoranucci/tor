package run

import (
	"errors"
	"fmt"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

type EventMapper struct {
	aggregateIDColumnName   string
	aggregateTypeColumnName string
	payloadColumnName       string
}

var notInsertError = errors.New("row-event is not an insert")

func (e *EventMapper) Map(event *canal.RowsEvent) ([]OutboxEvent, error) {
	if event.Action != canal.InsertAction {
		return nil, notInsertError
	}

	err := assertRowsSizesAreValid(event)
	if err != nil {
		return nil, err
	}

	oes := make([]OutboxEvent, 0, len(event.Rows))
	for _, row := range event.Rows {
		c := getColumns(event.Table.Columns, row)

		aggregateID, aggregateType, payload, err := e.getMainColumnsValue(c)
		if err != nil {
			return nil, err
		}

		oes = append(oes, OutboxEvent{
			AggregateID:                aggregateID,
			AggregateType:              aggregateType,
			Payload:                    payload,
			Columns:                    c,
			EventTimestampFromDatabase: event.Header.Timestamp,
		})
	}

	return oes, nil
}

func assertRowsSizesAreValid(event *canal.RowsEvent) error {
	for _, row := range event.Rows {
		if len(event.Table.Columns) != len(row) {
			return errors.New("unexpected row length")
		}
	}

	return nil
}

func getColumns(
	tableColumns []schema.TableColumn,
	rowColumns []interface{},
) []Column {
	r := make([]Column, 0, len(tableColumns))
	for i, etc := range tableColumns {
		if rowColumns[i] == nil {
			r = append(r, Column{
				Name:  []byte(etc.Name),
				Value: nil,
			})

			continue
		}

		cv, ok := rowColumns[i].([]byte)
		if !ok {
			cv = []byte(fmt.Sprintf("%v", rowColumns[i]))
		}

		r = append(r, Column{
			Name:  []byte(etc.Name),
			Value: cv,
		})
	}

	return r
}

func (e *EventMapper) getMainColumnsValue(
	columns []Column,
) ([]byte, []byte, []byte, error) {
	var aggregateID, aggregateType, payload []byte
	for _, column := range columns {
		if column.Name == nil {
			continue
		}

		switch string(column.Name) {
		case e.aggregateIDColumnName:
			aggregateID = column.Value
		case e.aggregateTypeColumnName:
			aggregateType = column.Value
		case e.payloadColumnName:
			payload = column.Value
		default:
			continue
		}
	}

	if aggregateID == nil {
		return nil, nil, nil, fmt.Errorf("%s Column not found", e.aggregateIDColumnName)
	}

	if aggregateType == nil {
		return nil, nil, nil, fmt.Errorf("%s Column not found", e.aggregateTypeColumnName)
	}

	if payload == nil {
		return nil, nil, nil, fmt.Errorf("%s Column not found", e.payloadColumnName)
	}

	return aggregateID, aggregateType, payload, nil
}
