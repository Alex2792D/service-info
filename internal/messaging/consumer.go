package messaging

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, group, redisURL, password string) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: group,
		Topic:   topic,
	})
	return &Consumer{reader: reader}
}

func (c *Consumer) Start(handle func(key []byte, value []byte)) {
	go func() {
		for {
			m, err := c.reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Kafka consumer error: %v", err)
				continue
			}
			handle(m.Key, m.Value)
		}
	}()
}

func (c *Consumer) Stop() {
	c.reader.Close()
}
