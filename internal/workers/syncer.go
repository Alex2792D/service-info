package workers

import (
	"context"
	"log"
	"time"

	"service-info/internal/kafka"

	"github.com/redis/go-redis/v9"
)

func StartUserSyncer(redisClient *redis.Client, consumer *kafka.Consumer) {
	if consumer == nil {
		return
	}
	consumer.Start(func(key, value []byte) {
		log.Printf("Обрабатываю Kafka-сообщение: key=%s, value=%s", key, value)
		userID := string(key)
		if userID == "" {
			log.Println("⚠️ UserSyncer: empty key")
			return
		}
		ctx := context.Background()
		redisKey := "user:" + userID
		if err := redisClient.Set(ctx, redisKey, value, 24*time.Hour).Err(); err != nil {
			log.Printf("UserSyncer: failed set %s: %v", redisKey, err)
			return
		}
		log.Printf("User cached in Redis: %s", redisKey)
	})
}
