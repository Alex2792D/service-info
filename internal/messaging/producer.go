package messaging

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic, username, password string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) Publish(key string, data interface{}) {
	go func() {
		bytes, err := json.Marshal(data)
		if err != nil {
			log.Printf("❌ Kafka marshal error for %s: %v", key, err)
			return
		}

		err = p.writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: bytes,
			},
		)
		if err != nil {
			log.Printf("❌ Kafka publish failed for %s: %v", key, err)
		} else {
			log.Printf("✅ Published to Kafka: %s", key)
		}
	}()
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
