package consumer

// Represents a consumer
type Consumer interface {
	Consume() (chan Message, error)
	Remove()
}

// Represents a message
type Message interface {
	Data() []byte
	Done(ack bool)
}
