package kafka

import (
	"strings"

	"transactional-outbox-go/internal/config"
	"transactional-outbox-go/internal/logger"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"go.uber.org/zap"
)

type Producer struct {
	producer *kafka.Producer
	topic    string
	dlqTopic string
}

func NewProducer(cfg *config.Config) (*Producer, error) {
	conf := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Kafka.Brokers, ","),
		"acks":              cfg.Kafka.Acks,
		"compression.type":  cfg.Kafka.Compression,
		"retries":           cfg.Kafka.Retries,
	}

	if cfg.Kafka.SecurityProto != "" {
		_ = conf.SetKey("security.protocol", cfg.Kafka.SecurityProto)
	}

	if cfg.Kafka.SASLMechanism != "" {
		_ = conf.SetKey("security.mechanism", cfg.Kafka.SASLMechanism)
	}

	if cfg.Kafka.SASLUsername != "" && cfg.Kafka.SASLPassword != "" {
		_ = conf.SetKey("security.username", cfg.Kafka.SASLUsername)
		_ = conf.SetKey("security.password", cfg.Kafka.SASLPassword)
	}

	p, err := kafka.NewProducer(conf)
	if err != nil {
		return nil, err
	}

	go func() {
		for events := range p.Events() {
			switch event := events.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					logger.L().Error("Message Delivery Failed", zap.Error(event.TopicPartition.Error))
				} else {
					logger.L().Debug("Message Delivery Successful", zap.String("topic", *event.TopicPartition.Topic), zap.String("metadata", *event.TopicPartition.Metadata))
				}
			}
		}
	}()

	return &Producer{
		producer: p,
		topic:    cfg.Kafka.Topic,
		dlqTopic: cfg.Kafka.DLQTopic,
	}, nil
}

func (p *Producer) SendMessage(key, value []byte) error {
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, nil)
}

func (p *Producer) SendDLQMessage(key, value []byte) error {
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.dlqTopic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
	}, nil)
}

func (p *Producer) Close() {
	p.producer.Flush(15 * 1000)
	p.producer.Close()
}
