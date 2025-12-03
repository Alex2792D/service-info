package messaging

import (
	"context"
	"crypto/tls"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type Consumer struct {
	reader *kafka.Reader
	stop   chan struct{}
}

func NewConsumer(brokers []string, topic, group, username, password string) *Consumer {
	mechanism := plain.Mechanism{
		Username: username,
		Password: password,
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: group,
		Dialer:  dialer,
	})

	return &Consumer{
		reader: reader,
		stop:   make(chan struct{}),
	}
}

func (c *Consumer) Start(handler func(key, value []byte)) {
	go func() {
		for {
			select {
			case <-c.stop:
				return
			default:
			}

			m, err := c.reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("❌ Kafka read error: %v", err)
				continue
			}

			handler(m.Key, m.Value)
		}
	}()
}

func (c *Consumer) Stop() {
	close(c.stop)
	c.reader.Close()
}
