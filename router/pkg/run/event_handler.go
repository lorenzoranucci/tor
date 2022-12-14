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

type outboxEvent struct {
	AggregateID string
	Payload     []byte
	Headers     []eventHeader
	Topic       string
}

type eventHeader struct {
	Key   []byte
	Value []byte
}

type EventDispatcher interface {
	Dispatch(
		topic string,
		routingKey string,
		event []byte,
		headers []struct {
			Key   []byte
			Value []byte
		},
	) error
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
	headersColumnsNames []string,
	aggregateTypeTopicPairs []AggregateTypeTopicPair,
	includeTransactionTimestamp bool,
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
			aggregateIDColumnName:       actualAggregateIDColumnName,
			aggregateTypeColumnName:     actualAggregateTypeColumnName,
			payloadColumnName:           actualPayloadColumnName,
			headersColumnsNames:         headersColumnsNames,
			aggregateTypeTopicPairs:     aggregateTypeTopicPairs,
			includeTransactionTimestamp: includeTransactionTimestamp,
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
		err = h.eventDispatcher.Dispatch(oe.Topic, oe.AggregateID, oe.Payload, mapHeaders(oe.Headers))
		if err != nil {
			return err
		}
		logrus.WithField("aggregateId", oe.AggregateID).
			WithField("payload", oe.Payload).
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

func mapHeaders(h []eventHeader) []struct {
	Key   []byte
	Value []byte
} {
	if len(h) == 0 {
		return nil
	}

	r := make([]struct {
		Key   []byte
		Value []byte
	}, 0, len(h))

	for _, v := range h {
		r = append(r, v)
	}

	return r
}
