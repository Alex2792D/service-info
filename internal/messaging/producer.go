package messaging

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Producer struct {
	topic  string
	client *kgo.Client
}

func NewProducer(brokers []string, topic string) *Producer {
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(&tls.Config{}),
		kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("❌ Failed to create Kafka producer: %v", err)
	}

	return &Producer{
		topic:  topic,
		client: client,
	}
}

func (p *Producer) Close() {
	p.client.Close()
}

func (p *Producer) Publish(key, value []byte) error {
	msg := &kgo.Record{
		Topic: p.topic,
		Key:   key,
		Value: value,
	}

	results := p.client.ProduceSync(context.Background(), msg)

	// Проверяем результаты
	for _, r := range results {
		if r.Err != nil {
			log.Printf("❌ Kafka publish error: %v", r.Err)
			return r.Err
		}
	}

	log.Printf("✅ Message published: key=%s", string(key))
	return nil
}
