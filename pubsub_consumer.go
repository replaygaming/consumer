package consumer

import (
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

const ContextDuration time.Duration = 30 * time.Second

// Google PubSub consumer and message implementation
type googlePubSubConsumer struct {
	Subscription *pubsub.Subscription
}

// Delegate to pubsub's Remove
func (c *googlePubSubConsumer) Remove() error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return c.Subscription.Delete(ctx)
}

type googlePubSubMessage struct {
	OriginalMessage *pubsub.Message
}

// Delegate to pubsub message's data
func (m *googlePubSubMessage) Data() []byte {
	return m.OriginalMessage.Data
}

// Delegate to pubsub message's Ack/Nack
func (m *googlePubSubMessage) Done(ack bool) {
	if ack == true {
		m.OriginalMessage.Ack()
	} else {
		m.OriginalMessage.Nack()
	}
}

var defaultProjectId = "emulator-project-id"

func newPubSubClient() (*pubsub.Client, error) {
	ctx := context.Background()
	projectId := os.Getenv("PUBSUB_PROJECT_ID")
	if projectId == "" {
		projectId = defaultProjectId
	}
	var client *pubsub.Client
	var err error

	// Create a new client with token
	keyfilePath := os.Getenv("PUBSUB_KEYFILE")

	if keyfilePath != "" {
		client, err = pubsub.NewClient(ctx, projectId, option.WithCredentialsFile(keyfilePath))
	} else {
		// Create client without token
		client, err = pubsub.NewClient(ctx, projectId)
	}

	return client, err
}

// Creates a new consumer
func NewConsumer(topicName string, subscriptionName string) Consumer {
	pubsubClient, err := newPubSubClient()

	if err != nil {
		log.Fatalf("Could not create PubSub client: %v", err)
	}

	topic := ensureTopic(pubsubClient, topicName)
	sub := ensureSubscription(pubsubClient, topic, subscriptionName)

	return &googlePubSubConsumer{Subscription: sub}
}

// Finds or creates a topic
func ensureTopic(pubsubClient *pubsub.Client, topicName string) *pubsub.Topic {
	var topic *pubsub.Topic
	ctx, _ := context.WithTimeout(context.Background(), ContextDuration)
	topic = pubsubClient.Topic(topicName)
	topicExists, err := topic.Exists(ctx)

	if err != nil {
		log.Fatalf("Could not check if topic exists: %v", err)
	}
	if !topicExists {
		new_topic, err := pubsubClient.CreateTopic(ctx, topicName)
		if err != nil {
			log.Fatalf("Could not create PubSub topic: %v", err)
		}
		topic = new_topic
	}

	return topic
}

// Finds or creates a subscription
func ensureSubscription(pubsubClient *pubsub.Client, topic *pubsub.Topic, subscriptionName string) *pubsub.Subscription {
	var subscription *pubsub.Subscription
	ctx, _ := context.WithTimeout(context.Background(), ContextDuration)
	subscription = pubsubClient.Subscription(subscriptionName)
	subscriptionExists, err := subscription.Exists(ctx)

	if err != nil {
		log.Fatalf("Could not check if subscription exists: %v", err)
	}
	if !subscriptionExists {
		new_subscription, err := pubsubClient.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			log.Fatalf("Could not create PubSub subscription: %v", err)
		}
		subscription = new_subscription
	}

	return subscription
}

// Creates a channel that pulls messages from the subscription
func (consumer *googlePubSubConsumer) Consume() (chan Message, error) {
	channel := make(chan Message)

	go func() {
		cctx, _ := context.WithTimeout(context.Background(), ContextDuration)

		err := consumer.Subscription.Receive(cctx,
			func(ctx context.Context, msg *pubsub.Message) {
				wrappedMsg := &googlePubSubMessage{OriginalMessage: msg}
				channel <- wrappedMsg
			})

		if err != nil {
			log.Fatalf("Could not receive message from subscription: %v", err)
		}
	}()

	return channel, nil
}
