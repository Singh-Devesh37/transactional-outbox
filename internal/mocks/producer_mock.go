package mocks

import (
	"sync"
)

// MockProducer is a mock implementation of kafka.MessageProducer for testing
type MockProducer struct {
	mu              sync.Mutex
	SendMessageFunc    func(key, value []byte) error
	SendDLQMessageFunc func(key, value []byte) error
	CloseFunc          func()

	// Track calls for assertions
	SendMessageCalls    []ProducerCall
	SendDLQMessageCalls []ProducerCall
	CloseCalled         bool
}

// ProducerCall represents a call to the producer
type ProducerCall struct {
	Key   []byte
	Value []byte
}

// NewMockProducer creates a new mock producer with default implementations
func NewMockProducer() *MockProducer {
	return &MockProducer{
		SendMessageFunc:    func(key, value []byte) error { return nil },
		SendDLQMessageFunc: func(key, value []byte) error { return nil },
		CloseFunc:          func() {},
	}
}

// SendMessage implements kafka.MessageProducer
func (m *MockProducer) SendMessage(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SendMessageCalls = append(m.SendMessageCalls, ProducerCall{Key: key, Value: value})
	if m.SendMessageFunc != nil {
		return m.SendMessageFunc(key, value)
	}
	return nil
}

// SendDLQMessage implements kafka.MessageProducer
func (m *MockProducer) SendDLQMessage(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SendDLQMessageCalls = append(m.SendDLQMessageCalls, ProducerCall{Key: key, Value: value})
	if m.SendDLQMessageFunc != nil {
		return m.SendDLQMessageFunc(key, value)
	}
	return nil
}

// Close implements kafka.MessageProducer
func (m *MockProducer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CloseCalled = true
	if m.CloseFunc != nil {
		m.CloseFunc()
	}
}

// Reset clears all recorded calls
func (m *MockProducer) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SendMessageCalls = nil
	m.SendDLQMessageCalls = nil
	m.CloseCalled = false
}

// GetSendMessageCallCount returns the number of SendMessage calls
func (m *MockProducer) GetSendMessageCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.SendMessageCalls)
}

// GetSendDLQMessageCallCount returns the number of SendDLQMessage calls
func (m *MockProducer) GetSendDLQMessageCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.SendDLQMessageCalls)
}
