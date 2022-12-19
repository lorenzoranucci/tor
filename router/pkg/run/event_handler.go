package run

import (
	"errors"
	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

const (
	defaultAggregateIDColumnName   = "aggregate_id"
	defaultAggregateTypeColumnName = "aggregate_type"
	defaultPayloadColumnName       = "payload"
)

type StateHandler interface {
	GetLastPosition() (mysql.Position, error)
	SetLastPosition(position mysql.Position) error
}

type OutboxEvent struct {
	AggregateID                []byte
	AggregateType              []byte
	Payload                    []byte
	Columns                    []Column
	EventTimestampFromDatabase uint32
}

type Column struct {
	Name  []byte
	Value []byte
}

type EventDispatcher interface {
	Dispatch(event OutboxEvent) error
}

type AggregateTypeTopicPair struct {
	AggregateTypeRegexp *regexp.Regexp
	Topic               string
}

func NewEventHandler(
	eventDispatcher EventDispatcher,
	aggregateIDColumnName string,
	aggregateTypeColumnName string,
	payloadColumnName string,
) (*EventHandler, error) {
	actualAggregateIDColumnName := defaultAggregateIDColumnName
	if aggregateIDColumnName != "" {
		actualAggregateIDColumnName = aggregateIDColumnName
	}

	actualAggregateTypeColumnName := defaultAggregateTypeColumnName
	if aggregateTypeColumnName != "" {
		actualAggregateTypeColumnName = aggregateTypeColumnName
	}

	actualPayloadColumnName := defaultPayloadColumnName
	if payloadColumnName != "" {
		actualPayloadColumnName = payloadColumnName
	}

	return &EventHandler{
		eventMapper: &EventMapper{
			aggregateIDColumnName:   actualAggregateIDColumnName,
			aggregateTypeColumnName: actualAggregateTypeColumnName,
			payloadColumnName:       actualPayloadColumnName,
		},
		eventDispatcher: eventDispatcher,
	}, nil
}

type EventHandler struct {
	canal.DummyEventHandler

	eventMapper     *EventMapper
	eventDispatcher EventDispatcher
	positionChan    chan mysql.Position
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	logrus.Debug("reading row-event")

	oes, err := h.eventMapper.Map(e)
	if err != nil && errors.Is(err, notInsertError) {
		logrus.Info("skipping row-event that is not an insert")
		return nil
	}
	if err != nil {
		return err
	}

	for _, oe := range oes {
		err = h.eventDispatcher.Dispatch(oe)
		if err != nil {
			return err
		}
		logrus.WithField("event", oe).
			Debug("event dispatched")
	}

	return err
}

func (h *EventHandler) OnPosSynced(p mysql.Position, g mysql.GTIDSet, f bool) error {
	h.positionChan <- p
	return nil
}

func (h *EventHandler) String() string {
	return "EventHandler"
}
