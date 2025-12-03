package messaging

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	redis  *redis.Client
}

func NewConsumer(brokers []string, topic, group string, redisClient *redis.Client) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  group,
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &Consumer{
		reader: reader,
		redis:  redisClient,
	}
}

func (c *Consumer) Start(handler func(key, value []byte)) {
	go func() {
		for {
			m, err := c.reader.FetchMessage(context.Background())
			if err != nil {
				log.Printf("Kafka consumer error: %v", err)
				continue
			}

			if handler != nil {
				handler(m.Key, m.Value)
			}

			c.reader.CommitMessages(context.Background(), m)
		}
	}()
}

func (c *Consumer) Stop() {
	c.reader.Close()
}
