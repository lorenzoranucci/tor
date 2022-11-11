package kafka

import (
	"fmt"

	"github.com/lorenzoranucci/transactional-outbox-router/pkg/kafka"
)

func NewEventDispatcher(producer *kafka.Producer) *EventDispatcher {
	return &EventDispatcher{producer: producer}
}

type EventDispatcher struct {
	producer *kafka.Producer
}

func (k *EventDispatcher) Dispatch(routingKey interface{}, message interface{}) error {
	return k.producer.Dispatch(fmt.Sprintf("%v", routingKey), fmt.Sprintf("%v", message))
}
