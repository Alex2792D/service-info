package messaging

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"service-info/internal/models"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	redis  *redis.Client
	stopCh chan struct{}
	doneCh chan struct{}
}

func NewConsumer(brokers []string, topic, groupID string, redis *redis.Client) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		}),
		redis:  redis,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (c *Consumer) Start() {
	go c.run()
}

func (c *Consumer) run() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			log.Println("🛑 Kafka consumer stopping...")
			return
		default:
			// ✅ Сначала читаем сообщение
			msg, err := c.reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("❌ Kafka read error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// ✅ Теперь msg определён и можно использовать
			var w models.Weather
			if err := json.Unmarshal(msg.Value, &w); err != nil {
				log.Printf("❌ Invalid message in Kafka: %v", err)
				continue
			}

			key := "weather:" + strings.ToLower(strings.TrimSpace(string(msg.Key)))
			if err := c.redis.Set(context.Background(), key, msg.Value, 10*time.Minute).Err(); err != nil {
				log.Printf("❌ Redis SET failed for %s: %v", key, err)
			} else {
				log.Printf("✅ Redis updated from Kafka: %s", key)
			}
		}
	}
}

func (c *Consumer) Stop() {
	close(c.stopCh)
	c.reader.Close()
	<-c.doneCh
	log.Println("✅ Kafka consumer stopped")
}
