package run

import (
	"errors"
	"regexp"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/sirupsen/logrus"
)

const (
	defaultAggregateIDColumnName   = "aggregate_id"
	defaultAggregateTypeColumnName = "aggregate_type"
	defaultPayloadColumnName       = "payload"
)

var defaultAggregateTypeRegexp = regexp.MustCompile(".*")

type StateHandler interface {
	GetLastPositionRead() (uint32, error)
	SetLastPositionRead(uint32) error
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
	lastPositionRead, err := stateHandler.GetLastPositionRead()
	if err != nil {
		return nil, err
	}

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
	if err != nil {
		return nil, err
	}

	return &EventHandler{
		stateHandler: stateHandler,
		eventMapper: &EventMapper{
			aggregateIDColumnName:   actualAggregateIDColumnName,
			aggregateTypeColumnName: actualAggregateTypeColumnName,
			payloadColumnName:       actualPayloadColumnName,
			aggregateTypeRegexp:     actualAggregateTypeRegexp,
		},
		eventDispatcher:  eventDispatcher,
		lastPositionRead: lastPositionRead,
	}, nil
}

type EventHandler struct {
	canal.DummyEventHandler

	stateHandler     StateHandler
	eventMapper      *EventMapper
	eventDispatcher  EventDispatcher
	lastPositionRead uint32
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	logrus.Debug("reading row-event")
	if h.lastPositionRead >= e.Header.LogPos {
		logrus.WithField("currentPositionRead", e.Header.LogPos).
			WithField("lastPositionRead", h.lastPositionRead).
			Info("skipping row-event that was already read")
		return nil
	}

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

	err = h.stateHandler.SetLastPositionRead(e.Header.LogPos)
	if err != nil {
		return err
	}
	logrus.WithField("lastPositionRead", e.Header.LogPos).
		Debug("last position read set")
	return err
}

func (h *EventHandler) String() string {
	return "EventHandler"
}
