package kafka

import (
	"github.com/lorenzoranucci/transactional-outbox-router/pkg/kafka"
)

func NewEventDispatcher(producer *kafka.Producer) *EventDispatcher {
	return &EventDispatcher{producer: producer}
}

type EventDispatcher struct {
	producer *kafka.Producer
}

func (k *EventDispatcher) Dispatch(routingKey string, event []byte) error {
	return k.producer.Dispatch(routingKey, event)
}
