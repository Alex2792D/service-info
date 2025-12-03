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

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) PublishWeather(city string, data any) {
	go func() {
		bytes, err := json.Marshal(data)
		if err != nil {
			log.Printf("❌ Kafka marshal error for %s: %v", city, err)
			return
		}

		err = p.writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(city),
			Value: bytes,
		})
		if err != nil {
			log.Printf("❌ Kafka publish failed for %s: %v", city, err)
		} else {
			log.Printf("✅ Published to Kafka: %s", city)
		}
	}()
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
