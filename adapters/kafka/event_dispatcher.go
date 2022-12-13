package kafka

import "github.com/Shopify/sarama"

func NewEventDispatcher(producer *Producer) *EventDispatcher {
	return &EventDispatcher{producer: producer}
}

type EventDispatcher struct {
	producer *Producer
}

func (k *EventDispatcher) Dispatch(
	routingKey string,
	event []byte,
	headers []struct {
		Key   []byte
		Value []byte
	},
) error {
	return k.producer.Dispatch(routingKey, event, mapHeaders(headers))
}

func mapHeaders(h []struct {
	Key   []byte
	Value []byte
}) []sarama.RecordHeader {
	r := make([]sarama.RecordHeader, 0, len(h))

	for _, v := range h {
		r = append(r, v)
	}

	return r
}
