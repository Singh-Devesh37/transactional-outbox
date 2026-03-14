package kafka

// MessageProducer defines the interface for Kafka message production
// This allows for easy mocking in tests
type MessageProducer interface {
	SendMessage(key, value []byte) error
	SendDLQMessage(key, value []byte) error
	Close()
}

// Ensure Producer implements MessageProducer
var _ MessageProducer = (*Producer)(nil)
