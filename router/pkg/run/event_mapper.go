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
	headersColumnsNames     []string
	aggregateTypeRegexp     *regexp.Regexp
}

var notInsertError = errors.New("row-event is not an insert")

func (e *EventMapper) Map(event *canal.RowsEvent) ([]outboxEvent, error) {
	if event.Action != canal.InsertAction {
		return nil, notInsertError
	}

	aggregateIDIndex, aggregateTypeIndex, payloadIndex, err := e.getMainColumnsIndices(event)
	if err != nil {
		return nil, err
	}

	headerColumnsIndices, err := e.getHeadersColumnsIndices(event, e.headersColumnsNames)
	if err != nil {
		return nil, err
	}

	oes := make([]outboxEvent, 0, len(event.Rows))
	for _, row := range event.Rows {
		err := assertRowSizeIsValid(len(row), []int{aggregateTypeIndex, aggregateIDIndex, payloadIndex}, headerColumnsIndices)
		if err != nil {
			return nil, err
		}

		aggregateID, aggregateType, payload, err := getMainColumnsValue(row, aggregateIDIndex, aggregateTypeIndex, payloadIndex)
		if err != nil {
			return nil, err
		}

		if !e.aggregateTypeRegexp.MatchString(aggregateType) {
			logrus.WithField("aggregateType", aggregateType).
				WithField("aggregateTypeRegexp", e.aggregateTypeRegexp.String()).
				Info("skipping outbox event that does not match aggregate type regexp")
			continue
		}

		h := getHeaderColumnsValues(row, headerColumnsIndices)

		oes = append(oes, outboxEvent{
			AggregateID: aggregateID,
			Payload:     payload,
			Headers:     h,
		})
	}

	return oes, nil
}

func assertRowSizeIsValid(rowLen int, columnIndices []int, headerColumnIndices []headerIndex) error {
	for _, index := range columnIndices {
		if index >= rowLen {
			return fmt.Errorf("unexpected event row size")
		}
	}

	for _, index := range headerColumnIndices {
		if index.index >= rowLen {
			return fmt.Errorf("unexpected event row size")
		}
	}

	return nil
}

func getMainColumnsValue(
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
		payloadS, ok := row[payloadIndex].(string)
		if !ok {
			return "", "", nil, fmt.Errorf("payload is not []byte or string")
		}
		payload = []byte(payloadS)
	}
	return aggregateID, aggregateType, payload, nil
}

func (e *EventMapper) getMainColumnsIndices(event *canal.RowsEvent) (int, int, int, error) {
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

func getHeaderColumnsValues(
	row []interface{},
	columnIndicesMap []headerIndex,
) []eventHeader {
	r := make([]eventHeader, 0, len(columnIndicesMap))
	for _, i := range columnIndicesMap {
		r = append(r, eventHeader{
			Key:   []byte(i.name),
			Value: []byte(fmt.Sprintf("%v", row[i.index])),
		})
	}

	return r
}

type headerIndex struct {
	name  string
	index int
}

func (e *EventMapper) getHeadersColumnsIndices(event *canal.RowsEvent, columnNames []string) ([]headerIndex, error) {
	r := make([]headerIndex, 0, len(columnNames))
outerLoop:
	for _, cm := range columnNames {
		for i, etc := range event.Table.Columns {
			if etc.Name == cm {
				r = append(r, headerIndex{
					name:  cm,
					index: i,
				})
				continue outerLoop
			}
		}

		return nil, fmt.Errorf("column not found with name: %s", cm)
	}

	return r, nil
}
