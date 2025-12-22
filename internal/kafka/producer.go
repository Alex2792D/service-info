package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ProducerInterface interface {
	PublishObjectAsync(key []byte, obj interface{})
}
type Producer struct {
	topic  string
	client *kgo.Client
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func NewProducer(topic string) *Producer {
	brokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
	kafkaEnv := getEnv("KAFKA_ENV", "local")

	var opts []kgo.Opt
	opts = append(opts, kgo.SeedBrokers(brokers...))

	if kafkaEnv == "cloud" {
		username := os.Getenv("KAFKA_USERNAME")
		password := os.Getenv("KAFKA_PASSWORD")
		if username == "" || password == "" {
			log.Fatal("❌ KAFKA_USERNAME и KAFKA_PASSWORD обязательны для KAFKA_ENV=cloud")
		}
		log.Fatal("Cloud Kafka config not implemented yet — only 'local' supported")
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("❌ Failed to create Kafka producer: %v", err)
	}

	log.Printf("✅ Kafka producer initialized for topic: %s (env=%s)", topic, kafkaEnv)
	return &Producer{topic: topic, client: client}
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := p.client.ProduceSync(ctx, msg)
	for _, r := range results {
		if r.Err != nil {
			log.Printf(" Kafka publish error: %v", r.Err)
			return r.Err
		}
	}

	log.Printf("Published to %s: key=%s", p.topic, string(key))
	return nil
}

func (p *Producer) PublishAsync(key, value []byte) {
	go func() {
		if err := p.Publish(key, value); err != nil {
			log.Printf("Kafka async publish error: %v", err)
		}
	}()
}

func (p *Producer) PublishObjectAsync(key []byte, obj interface{}) {
	go func() {
		value, err := json.Marshal(obj)
		if err != nil {
			log.Printf("Failed to marshal object for Kafka: %v", err)
			return
		}

		if err := p.Publish(key, value); err != nil {
			log.Printf("Kafka async publish error: %v", err)
		}
	}()
}
