package messaging

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Producer struct {
	topic  string
	client *kgo.Client
}

func NewProducer(topic string) *Producer {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	if brokers[0] == "" || username == "" || password == "" {
		log.Fatal("❌ KAFKA_BROKERS, KAFKA_USERNAME или KAFKA_PASSWORD не установлены")
	}

	// TLS конфиг для Redpanda Cloud
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // для теста на Render, потом лучше указать Root CA
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("❌ Failed to create Kafka producer: %v", err)
	}

	log.Printf("✅ Kafka producer initialized for topic: %s", topic)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	results := p.client.ProduceSync(ctx, msg)

	for _, r := range results {
		if r.Err != nil {
			log.Printf("❌ Kafka publish error: %v", r.Err)
			return r.Err
		}
	}

	log.Printf("✅ Message published: key=%s", string(key))
	return nil
}
