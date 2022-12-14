package kafka

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
	syncProducer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	syncProducer, err := newSyncProducer(brokers)
	if err != nil {
		return nil, err
	}

	return &Producer{syncProducer: syncProducer}, nil
}

func (p *Producer) Dispatch(
	topic string,
	key string,
	message []byte,
	headers []sarama.RecordHeader,
) error {
	_, _, err := p.syncProducer.SendMessage(
		&sarama.ProducerMessage{
			Key:     sarama.StringEncoder(key),
			Topic:   topic,
			Value:   sarama.ByteEncoder(message),
			Headers: headers,
		},
	)

	return err
}

func newSyncProducer(brokerList []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}

	return producer, err
}

func (p *Producer) Close() error {
	return p.syncProducer.Close()
}
