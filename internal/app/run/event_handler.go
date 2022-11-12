package run

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

func NewEventHandler(
	stateHandler StateHandler,
	eventSerializer EventSerializer,
	eventKeySerializer EventKeySerializer,
	eventDispatcher EventDispatcher,
) (*EventHandler, error) {
	lastPositionRead, err := stateHandler.GetLastPositionRead()
	if err != nil {
		return nil, err
	}

	return &EventHandler{
		stateHandler:       stateHandler,
		eventSerializer:    eventSerializer,
		eventKeySerializer: eventKeySerializer,
		eventDispatcher:    eventDispatcher,
		lastPositionRead:   lastPositionRead,
	}, nil
}

type EventHandler struct {
	canal.DummyEventHandler

	stateHandler       StateHandler
	eventSerializer    EventSerializer
	eventKeySerializer EventKeySerializer
	eventDispatcher    EventDispatcher
	lastPositionRead   uint32
}

type StateHandler interface {
	GetLastPositionRead() (uint32, error)
	SetLastPositionRead(uint32) error
}

type EventSerializer interface {
	SerializeMessage(e *canal.RowsEvent) (interface{}, error)
}

type EventKeySerializer interface {
	SerializeKey(e *canal.RowsEvent) (interface{}, error)
}

type EventDispatcher interface {
	Dispatch(routingKey interface{}, message interface{}) error
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	if h.lastPositionRead >= e.Header.LogPos {
		return nil
	}

	k, err := h.eventKeySerializer.SerializeKey(e)
	if err != nil {
		return err
	}

	m, err := h.eventSerializer.SerializeMessage(e)
	if err != nil {
		return err
	}

	err = h.eventDispatcher.Dispatch(k, m)
	if err != nil {
		return err
	}

	return h.stateHandler.SetLastPositionRead(e.Header.LogPos)
}

func (h *EventHandler) String() string {
	return "EventHandler"
}
