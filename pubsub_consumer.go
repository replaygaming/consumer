package consumer

import (
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/pubsub"
	"io/ioutil"
	"log"
	"os"
)

// Google PubSub consumer and message implementation
type googlePubSubConsumer struct {
	Subscription *pubsub.Subscription
}

// Delegate to pubsub's Remove
func (c *googlePubSubConsumer) Remove() {
	c.Subscription.Delete(context.Background())
}

type googlePubSubMessage struct {
	OriginalMessage *pubsub.Message
}

// Delegate to pubsub message's data
func (m *googlePubSubMessage) Data() []byte {
	return m.OriginalMessage.Data
}

// Delegate to pubsub message's Done
func (m *googlePubSubMessage) Done(ack bool) {
	m.OriginalMessage.Done(ack)
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
		jsonKey, err := ioutil.ReadFile(keyfilePath)
		if err != nil {
			return nil, err
		}
		conf, err := google.JWTConfigFromJSON(
			jsonKey,
			pubsub.ScopeCloudPlatform,
			pubsub.ScopePubSub,
		)

		if err != nil {
			return nil, err
		}
		tokenSource := conf.TokenSource(ctx)
		client, err = pubsub.NewClient(ctx, projectId, cloud.WithTokenSource(tokenSource))
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
	topic = pubsubClient.Topic(topicName)
	topicExists, _ := topic.Exists(context.Background())

	if !topicExists {
		new_topic, err := pubsubClient.NewTopic(context.Background(), topicName)
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
	subscription = pubsubClient.Subscription(subscriptionName)
	subscriptionExists, _ := subscription.Exists(context.Background())

	if !subscriptionExists {
		new_subscription, err := pubsubClient.NewSubscription(context.Background(), subscriptionName, topic, 0, nil)
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
		it, err := consumer.Subscription.Pull(context.Background())
		if err != nil {
			log.Printf("Could not pull message from subscription: %v", err)
			return
		}
		defer it.Stop()

		for {
			msg, err := it.Next()
			if err == pubsub.Done {
				break
			}
			if err != nil {
				log.Printf("Error consuming messages: %v", err)
				break
			}

			wrappedMsg := &googlePubSubMessage{OriginalMessage: msg}

			channel <- wrappedMsg
		}
	}()

	return channel, nil
}
