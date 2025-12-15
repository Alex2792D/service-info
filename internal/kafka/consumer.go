package kafka

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client *kgo.Client
	topic  string
}

func NewConsumer(topic, group string) *Consumer {
	brokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
	kafkaEnv := getEnv("KAFKA_ENV", "local")

	var opts []kgo.Opt
	opts = append(opts,
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	if kafkaEnv == "cloud" {
		log.Fatal("Cloud Kafka config not implemented yet — only 'local' supported")
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}

	log.Printf("Kafka consumer initialized for topic: %s, group: %s (env=%s)", topic, group, kafkaEnv)
	return &Consumer{client: client, topic: topic}
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
				handler(record.Key, record.Value)
			}
		}
	}()
}

func (c *Consumer) Stop() {
	c.client.Close()
}
