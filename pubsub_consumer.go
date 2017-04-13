package consumer

import (
	"io/ioutil"
	"log"
	"os"
	"time"
	"sync"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

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
		client, err = pubsub.NewClient(ctx, projectId, option.WithTokenSource(tokenSource))
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
	topicExists, err := topic.Exists(context.Background())

	if err != nil {
		log.Fatalf("PubSub topic does not exist: %v", err)
	}
	if !topicExists {
		new_topic, err := pubsubClient.CreateTopic(context.Background(), topicName)
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
	subscriptionExists, err := subscription.Exists(context.Background())

	if err != nil {
		log.Fatalf("PubSub subscription does not exist: %v", err)
	}
	if !subscriptionExists {
		new_subscription, err := pubsubClient.CreateSubscription(context.Background(), subscriptionName, topic, 0, nil)
		if err != nil {
			log.Fatalf("Could not create PubSub subscription: %v", err)
		}
		subscription = new_subscription
	}

	return subscription
}

// Creates a channel that pulls messages from the subscription
func (consumer *googlePubSubConsumer) Consume() (chan Message, error) {
	var mu sync.Mutex
	channel := make(chan Message)

	go func() {
		cctx, cancel := context.WithCancel(context.Background())

		err := consumer.Subscription.Receive(cctx,
			func(ctx context.Context, msg *pubsub.Message) {
				mu.Lock()
				defer mu.Unlock()

				wrappedMsg := &googlePubSubMessage{OriginalMessage: msg}
				channel <- wrappedMsg
				msg.Ack()
			})

		if err != nil {
			cancel()
			log.Fatalf("Could not receive message from subscription: %v", err)
		}
	}()

	return channel, nil
}

func (consumer *googlePubSubConsumer) Alive() bool {
	ok, err := consumer.Subscription.Exists(context.Background())
	if err != nil || !ok {
		return false
	}
	return true
}
