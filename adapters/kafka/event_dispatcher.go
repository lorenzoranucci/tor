package kafka

func NewEventDispatcher(producer *Producer) *EventDispatcher {
	return &EventDispatcher{producer: producer}
}

type EventDispatcher struct {
	producer *Producer
}

func (k *EventDispatcher) Dispatch(routingKey string, event []byte) error {
	return k.producer.Dispatch(routingKey, event)
}
