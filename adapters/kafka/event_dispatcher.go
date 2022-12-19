package kafka

import (
	"fmt"
	"regexp"

	"github.com/Shopify/sarama"
	"github.com/lorenzoranucci/tor/router/pkg/run"
)

func NewEventDispatcher(
	syncProducer sarama.SyncProducer,
	admin sarama.ClusterAdmin,
	topics []Topic,
	headerMappings []HeaderMapping,
) (*EventDispatcher, error) {
	err := createTopics(topics, admin)
	if err != nil {
		return nil, err
	}

	return &EventDispatcher{
		syncProducer:   syncProducer,
		admin:          admin,
		topics:         topics,
		headerMappings: headerMappings,
	}, nil
}

type EventDispatcher struct {
	syncProducer   sarama.SyncProducer
	admin          sarama.ClusterAdmin
	topics         []Topic
	headerMappings []HeaderMapping
}

type Topic struct {
	Name          string
	TopicDetail   *sarama.TopicDetail
	AggregateType *regexp.Regexp
}

type HeaderMapping struct {
	ColumnName string
	HeaderName string
}

func (k *EventDispatcher) Dispatch(event run.OutboxEvent) error {
	for _, topic := range k.topics {
		if !topic.AggregateType.MatchString(string(event.AggregateType)) {
			continue
		}

		headers, err := k.mapHeaders(event.Columns)
		if err != nil {
			return err
		}

		_, _, err = k.syncProducer.SendMessage(
			&sarama.ProducerMessage{
				Key:     sarama.ByteEncoder(event.AggregateID),
				Topic:   topic.Name,
				Value:   sarama.ByteEncoder(event.Payload),
				Headers: headers,
			},
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func createTopics(topics []Topic, admin sarama.ClusterAdmin) error {
	topicNames := make([]string, 0, len(topics))
	for _, topic := range topics {
		topicNames = append(topicNames, topic.Name)
	}

	topicMetadata, err := admin.DescribeTopics(topicNames)
	if err != nil {
		return err
	}

	for _, m := range topicMetadata {
		if m.Err != sarama.ErrUnknownTopicOrPartition {
			continue
		}

		for _, topic := range topics {
			if topic.Name != m.Name {
				continue
			}

			err := admin.CreateTopic(topic.Name, topic.TopicDetail, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (k *EventDispatcher) mapHeaders(columns []run.Column) ([]sarama.RecordHeader, error) {
	r := make([]sarama.RecordHeader, 0, len(columns))

outerLoop:
	for _, h := range k.headerMappings {
		for _, c := range columns {
			if h.ColumnName == string(c.Name) {
				r = append(r, sarama.RecordHeader{
					Key:   []byte(h.HeaderName),
					Value: c.Value,
				})

				continue outerLoop
			}
		}

		return nil, fmt.Errorf("column not found for header. Column: %s, Header: %s", h.ColumnName, h.HeaderName)
	}

	return r, nil
}
