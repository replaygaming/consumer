package consumer

// Represents a consumer
type Consumer interface {
	// Creates a channel that will receive messages. Each message should be
	// acknowledged once you're done with it through the Done() method on the
	// message
	Consume() (chan Message, error)

	// Signals the backend to remove this consumer
	Remove()
}

// Represents a message
type Message interface {
	// Returns the data inside of the message
	Data() []byte
	// Marks that the message has been processed. If ack is true, it also
	// acknowledges that the message should no longer be received.
	Done(ack bool)
}
