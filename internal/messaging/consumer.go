package messaging

import (
	"context"
	"crypto/tls"
	"log"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Consumer struct {
	client *kgo.Client
	topic  string
}

func NewConsumer(topic, group string) *Consumer {
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	if brokers[0] == "" || username == "" || password == "" {
		log.Fatal("❌ KAFKA_BROKERS, KAFKA_USERNAME или KAFKA_PASSWORD не установлены")
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // для теста на Render
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DialTLSConfig(tlsConfig),
		kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // читаем с начала
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("❌ Failed to create Kafka consumer: %v", err)
	}

	log.Printf("✅ Kafka consumer initialized for topic: %s, group: %s", topic, group)
	return &Consumer{
		client: client,
		topic:  topic,
	}
}

func (c *Consumer) Start(handler func(key, value []byte)) {
	go func() {
		log.Printf("🔄 Consumer goroutine STARTED for topic: %s", c.topic)
		for {
			log.Printf("📥 Polling fetches for %s...", c.topic)
			fetches := c.client.PollFetches(context.Background())
			log.Printf("📥 Fetched %d partitions", len(fetches))
			if errs := fetches.Errors(); len(errs) > 0 {
				log.Printf("❌ Kafka fetch errors: %v", errs)
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if handler != nil {
					handler(record.Key, record.Value)
				}
			}
		}
	}()
}

func (c *Consumer) Stop() {
	c.client.Close()
}

// package messaging

// import (
// 	"context"
// 	"log"

// 	"github.com/twmb/franz-go/pkg/kgo"
// )

// type Consumer struct {
// 	client *kgo.Client
// 	topic  string
// }

// func NewConsumer(topic, group string) *Consumer {
// 	brokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
// 	kafkaEnv := getEnv("KAFKA_ENV", "local")

// 	var opts []kgo.Opt
// 	opts = append(opts,
// 		kgo.SeedBrokers(brokers...),
// 		kgo.ConsumeTopics(topic),
// 		kgo.ConsumerGroup(group),
// 		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
// 	)

// 	// Для локальной Kafka — никаких TLS/SASL
// 	if kafkaEnv == "cloud" {
// 		log.Fatal("Cloud Kafka config not implemented yet — only 'local' supported")
// 	}

// 	client, err := kgo.NewClient(opts...)
// 	if err != nil {
// 		log.Fatalf("❌ Failed to create Kafka consumer: %v", err)
// 	}

// 	log.Printf("✅ Kafka consumer initialized for topic: %s, group: %s (env=%s)", topic, group, kafkaEnv)
// 	return &Consumer{client: client, topic: topic}
// }

// func (c *Consumer) Start(handler func(key, value []byte)) {
// 	go func() {
// 		for {
// 			fetches := c.client.PollFetches(context.Background())
// 			if errs := fetches.Errors(); len(errs) > 0 {
// 				log.Printf("❌ Kafka fetch errors: %v", errs)
// 			}
// 			iter := fetches.RecordIter()
// 			for !iter.Done() {
// 				record := iter.Next()
// 				handler(record.Key, record.Value)
// 			}
// 		}
// 	}()
// }

// func (c *Consumer) Stop() {
// 	c.client.Close()
// }
