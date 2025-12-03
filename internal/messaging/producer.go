package messaging

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic, username, password string) *Producer {
	mechanism := plain.Mechanism{
		Username: username,
		Password: password,
	}

	transport := &kafka.Transport{
		SASL: mechanism,
		TLS:  &tls.Config{},
	}

	return &Producer{
		writer: &kafka.Writer{
			Addr:      kafka.TCP(brokers...),
			Topic:     topic,
			Balancer:  &kafka.LeastBytes{},
			Transport: transport,
		},
	}
}

func (p *Producer) Publish(key string, data interface{}) {
	go func() {
		bytes, err := json.Marshal(data)
		if err != nil {
			log.Printf("❌ Kafka marshal error: %v", err)
			return
		}

		err = p.writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: bytes,
			},
		)

		if err != nil {
			log.Printf("❌ Kafka publish failed: %v", err)
		} else {
			log.Printf("📨 Kafka published: %s", key)
		}
	}()
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
