package messaging

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	topic  string
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		topic: topic,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) Close() {
	if err := p.writer.Close(); err != nil {
		log.Printf("❌ Kafka producer close error: %v", err)
	}
}

// Publish публикует ключ и значение в Kafka
func (p *Producer) Publish(key []byte, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}
	if err := p.writer.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("❌ Kafka publish error: %v", err)
		return err
	}
	log.Printf("✅ Message published to Kafka: key=%s", string(key))
	return nil
}
