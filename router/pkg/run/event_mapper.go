package run

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/sirupsen/logrus"
)

type EventMapper struct {
	aggregateIDColumnName   string
	aggregateTypeColumnName string
	payloadColumnName       string
	aggregateTypeRegexp     *regexp.Regexp
}

var notInsertError = errors.New("row-event is not an insert")

func (e *EventMapper) Map(event *canal.RowsEvent) ([]OutboxEvent, error) {
	if event.Action != canal.InsertAction {
		return nil, notInsertError
	}

	aggregateIDIndex, aggregateTypeIndex, payloadIndex, err := e.getColumnsIndex(event)
	if err != nil {
		return nil, err
	}

	oes := make([]OutboxEvent, 0, len(event.Rows))
	for _, row := range event.Rows {
		err := assertRowSizeIsValid(len(row), []int{aggregateTypeIndex, aggregateIDIndex, payloadIndex})
		if err != nil {
			return nil, err
		}

		aggregateID, aggregateType, payload, err := getColumnsValue(row, aggregateIDIndex, aggregateTypeIndex, payloadIndex)
		if err != nil {
			return nil, err
		}

		if !e.aggregateTypeRegexp.MatchString(aggregateType) {
			logrus.WithField("aggregateType", aggregateType).
				WithField("aggregateTypeRegexp", e.aggregateTypeRegexp.String()).
				Info("skipping outbox event that does not match aggregate type regexp")
			continue
		}

		oes = append(oes, OutboxEvent{
			AggregateID: aggregateID,
			Payload:     payload,
		})
	}

	return oes, nil
}

func assertRowSizeIsValid(rowLen int, columnIndices []int) error {
	for _, index := range columnIndices {
		if index >= rowLen {
			return fmt.Errorf("unexpected event row size")
		}
	}

	return nil
}

func getColumnsValue(
	row []interface{},
	aggregateIDIndex int,
	aggregateTypeIndex int,
	payloadIndex int,
) (string, string, []byte, error) {
	aggregateID, ok := row[aggregateIDIndex].(string)
	if !ok {
		return "", "", nil, fmt.Errorf("aggregate id is not string")
	}

	aggregateType, ok := row[aggregateTypeIndex].(string)
	if !ok {
		return "", "", nil, fmt.Errorf("aggregate type is not string")
	}

	payload, ok := row[payloadIndex].([]byte)
	if !ok {
		return "", "", nil, fmt.Errorf("payload is not []byte")
	}
	return aggregateID, aggregateType, payload, nil
}

func (e *EventMapper) getColumnsIndex(event *canal.RowsEvent) (int, int, int, error) {
	aggregateIDIndex := -1
	aggregateTypeIndex := -1
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

		if column.Name == e.aggregateTypeColumnName {
			aggregateTypeIndex = i
			continue
		}
	}

	if aggregateIDIndex == -1 {
		return -1, -1, -1, fmt.Errorf("outbox table has no '%s' column", e.aggregateIDColumnName)
	}

	if aggregateTypeIndex == -1 {
		return -1, -1, -1, fmt.Errorf("outbox table has no '%s' column", e.aggregateTypeColumnName)
	}

	if payloadIndex == -1 {
		return -1, -1, -1, fmt.Errorf("outbox table has no '%s' column", e.payloadColumnName)
	}

	return aggregateIDIndex, aggregateTypeIndex, payloadIndex, nil
}
