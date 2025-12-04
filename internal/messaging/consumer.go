package messaging

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Consumer struct {
	client *kgo.Client
	topic  string
}

func NewConsumer(topic, group string) *Consumer {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(&tls.Config{}),
		kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("❌ Failed to create Kafka consumer: %v", err)
	}

	return &Consumer{
		client: client,
		topic:  topic,
	}
}

func (c *Consumer) Start(handler func(key, value []byte)) {
	go func() {
		for {
			fetches := c.client.PollFetches(context.Background())
			if errs := fetches.Errors(); len(errs) > 0 {
				log.Printf("Kafka fetch errors: %v", errs)
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if handler != nil {
					handler(record.Key, record.Value)
				}
			}
		}
	}()
}

func (c *Consumer) Stop() {
	c.client.Close()
}
