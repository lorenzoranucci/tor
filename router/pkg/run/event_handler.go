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

var defaultAggregateTypeRegexp = regexp.MustCompile(".*")

type StateHandler interface {
	GetLastPosition() (mysql.Position, error)
	SetLastPosition(position mysql.Position) error
}

type OutboxEvent struct {
	AggregateID string
	Payload     []byte
}

type EventDispatcher interface {
	Dispatch(routingKey string, event []byte) error
}

func NewEventHandler(
	stateHandler StateHandler,
	eventDispatcher EventDispatcher,
	aggregateIDColumnName string,
	aggregateTypeColumnName string,
	payloadColumnName string,
	aggregateTypeRegexp *regexp.Regexp,
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

	actualAggregateTypeRegexp := defaultAggregateTypeRegexp
	if aggregateTypeRegexp != nil {
		actualAggregateTypeRegexp = aggregateTypeRegexp
	}

	return &EventHandler{
		stateHandler: stateHandler,
		eventMapper: &EventMapper{
			aggregateIDColumnName:   actualAggregateIDColumnName,
			aggregateTypeColumnName: actualAggregateTypeColumnName,
			payloadColumnName:       actualPayloadColumnName,
			aggregateTypeRegexp:     actualAggregateTypeRegexp,
		},
		eventDispatcher: eventDispatcher,
	}, nil
}

type EventHandler struct {
	canal.DummyEventHandler

	stateHandler    StateHandler
	eventMapper     *EventMapper
	eventDispatcher EventDispatcher
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
		err = h.eventDispatcher.Dispatch(oe.AggregateID, oe.Payload)
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
	return h.setLastPosition(p)
}

func (h *EventHandler) String() string {
	return "EventHandler"
}

func (h *EventHandler) setLastPosition(p mysql.Position) error {
	err := h.stateHandler.SetLastPosition(p)
	if err != nil {
		return err
	}
	logrus.WithField("lastPosition", p).
		Debug("last position set")

	return nil
}
