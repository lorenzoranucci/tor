package run

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

func NewEventHandler(
	stateHandler StateHandler,
	rowEventSerializer EventSerializer,
	eventDispatcher EventDispatcher,
) (*EventHandler, error) {
	lastPositionRead, err := stateHandler.GetLastPositionRead()
	if err != nil {
		return nil, err
	}

	return &EventHandler{
		stateHandler:       stateHandler,
		rowEventSerializer: rowEventSerializer,
		eventDispatcher:    eventDispatcher,
		lastPositionRead:   lastPositionRead,
	}, nil
}

type EventHandler struct {
	canal.DummyEventHandler

	stateHandler       StateHandler
	rowEventSerializer EventSerializer
	eventDispatcher    EventDispatcher
	lastPositionRead   uint32
}

type StateHandler interface {
	GetLastPositionRead() (uint32, error)
	SetLastPositionRead(uint32) error
}

type EventSerializer interface {
	SerializeKey(e *canal.RowsEvent) (interface{}, error)
	SerializeMessage(e *canal.RowsEvent) (interface{}, error)
}

type EventDispatcher interface {
	Dispatch(routingKey interface{}, message interface{}) error
}

func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	if h.lastPositionRead >= e.Header.LogPos {
		return nil
	}

	k, err := h.rowEventSerializer.SerializeKey(e)
	if err != nil {
		return err
	}

	m, err := h.rowEventSerializer.SerializeMessage(e)
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
