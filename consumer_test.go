package consumer

import (
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
)

var _ = func() bool {
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		os.Setenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8538")
	}
	return true
}()

func TestConsume(t *testing.T) {
	pubsubClient, _ := pubsub.NewClient(context.Background(), "emulator-project-id")
	topic := pubsubClient.Topic("test-topic")

	c := NewConsumer("test-topic", "test-subscription")
	messagesChannel, _ := c.Consume()

	// Send two messages
	topic.Publish(context.Background(), &pubsub.Message{
		Data: []byte("test"),
	})
	topic.Publish(context.Background(), &pubsub.Message{
		Data: []byte("test2"),
	})

	expected := [][]byte{[]byte("test"), []byte("test2")}
	receivedChannel := make(chan []byte)

	go func() {
		for m := range messagesChannel {
			receivedChannel <- m.Data()
			m.Done(true)
		}
	}()

	// Receive 2 messages, timing out after 1 second
	var received [][]byte
	for {
		select {
		case msg := <-receivedChannel:
			received = append(received, msg)
		case <-time.After(time.Second * 1):
			break
		}

		if len(received) >= 2 {
			break
		}
	}

	// Verify all messages arrived independent of the order
	for _, receivedMsg := range received {
		if !inArray(receivedMsg, expected) {
			t.Errorf("Expected %v to be included in %v", received, expected)
		}
	}
}

func TestRemove(t *testing.T) {
	pubsubClient, _ := pubsub.NewClient(context.Background(), "emulator-project-id")
	topic := pubsubClient.Topic("test-topic")
	subscription := pubsubClient.Subscription("test-subscription")

	c := NewConsumer("test-topic", "test-subscription")
	c.Remove()

	subscriptionExists, _ := subscription.Exists(context.Background())
	if subscriptionExists {
		t.Error("Expected subscription to be removed")
	}

	topicExists, _ := topic.Exists(context.Background())
	if !topicExists {
		t.Error("Expected topic to not be removed")
	}
}

func inArray(msg []byte, array [][]byte) bool {
	for _, item := range array {
		if reflect.DeepEqual(msg, item) {
			return true
		}
	}
	return false
}
