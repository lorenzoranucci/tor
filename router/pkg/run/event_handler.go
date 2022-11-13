package run

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

const (
	defaultAggregateIDColumnName = "aggregate_id"
	defaultPayloadColumnName     = "payload"
)

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
	payloadColumnName string,
) (*EventHandler, error) {
	lastPositionRead, err := stateHandler.GetLastPositionRead()
	if err != nil {
		return nil, err
	}

	actualAggregateIDColumnName := defaultAggregateIDColumnName
	if aggregateIDColumnName != "" {
		actualAggregateIDColumnName = aggregateIDColumnName
	}

	actualPayloadColumnName := defaultPayloadColumnName
	if payloadColumnName != "" {
		actualPayloadColumnName = payloadColumnName
	}

	return &EventHandler{
		stateHandler: stateHandler,
		eventMapper: &EventMapper{
			aggregateIDColumnName: actualAggregateIDColumnName,
			payloadColumnName:     actualPayloadColumnName,
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
	if h.lastPositionRead >= e.Header.LogPos {
		return nil
	}

	oes, err := h.eventMapper.Map(e)
	if err != nil {
		return err
	}

	for _, oe := range oes {
		err = h.eventDispatcher.Dispatch(oe.AggregateID, oe.Payload)
		if err != nil {
			return err
		}
	}

	return h.stateHandler.SetLastPositionRead(e.Header.LogPos)
}

func (h *EventHandler) String() string {
	return "EventHandler"
}
