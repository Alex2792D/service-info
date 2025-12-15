package workers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"service-info/internal/kafka"
	"service-info/internal/models"

	"github.com/redis/go-redis/v9"
)

type WorkerBundle struct {
	WeatherWorker  *WeatherWorker
	ExchangeWorker *ExchangeWorker
}

func StartAllWorkers(ctx context.Context, redisClient *redis.Client, kafkaBundle *kafka.KafkaBundle) *WorkerBundle {
	weatherCh := make(chan []byte, 100)
	exchangeCh := make(chan []byte, 100)

	StartWorkerMultiplexer(kafkaBundle.PopularConsumer, weatherCh, exchangeCh)
	startUserSyncer(redisClient, kafkaBundle.UserConsumer)
	startWeatherSyncer(redisClient, kafkaBundle.WeatherConsumer)
	startExchangeSyncer(redisClient, kafkaBundle.ExchangeConsumer)

	weatherWorker := NewWeatherWorker(weatherCh, redisClient)
	exchangeWorker := NewExchangeWorker(exchangeCh, redisClient)

	go weatherWorker.Start(ctx)
	go exchangeWorker.Start(ctx)

	return &WorkerBundle{
		WeatherWorker:  weatherWorker,
		ExchangeWorker: exchangeWorker,
	}
}

func startUserSyncer(redisClient *redis.Client, consumer *kafka.Consumer) {
	if consumer == nil {
		return
	}
	consumer.Start(func(key, value []byte) {
		userID := string(key)
		if userID == "" {
			log.Println("⚠️ UserSyncer: empty key")
			return
		}
		ctx := context.Background()
		redisKey := "user:" + userID
		if err := redisClient.Set(ctx, redisKey, value, 24*time.Hour).Err(); err != nil {
			log.Printf(" UserSyncer: failed set %s: %v", redisKey, err)
			return
		}
		log.Printf("User cached in Redis: %s", redisKey)
	})
}

func startWeatherSyncer(redisClient *redis.Client, consumer *kafka.Consumer) {
	if consumer == nil {
		return
	}
	consumer.Start(func(key, value []byte) {
		if len(key) == 0 {
			log.Println("⚠️ WeatherSyncer: empty key")
			return
		}
		var weather models.Weather
		if err := json.Unmarshal(value, &weather); err != nil {
			log.Printf(" WeatherSyncer: unmarshal failed: %v", err)
			return
		}
		ctx := context.Background()
		redisKey := string(key)
		if err := redisClient.Set(ctx, redisKey, value, 10*time.Minute).Err(); err != nil {
			log.Printf("WeatherSyncer: failed set %s: %v", redisKey, err)
			return
		}
		log.Printf("Weather cached in Redis: %s", redisKey)
	})
}

func startExchangeSyncer(redisClient *redis.Client, consumer *kafka.Consumer) {
	if consumer == nil {
		return
	}
	consumer.Start(func(key, value []byte) {
		if len(key) == 0 {
			log.Println("ExchangeSyncer: empty key")
			return
		}
		var rate models.ExchangeRate
		if err := json.Unmarshal(value, &rate); err != nil {
			log.Printf("ExchangeSyncer: unmarshal failed: %v", err)
			return
		}
		ctx := context.Background()
		redisKey := string(key)
		if err := redisClient.Set(ctx, redisKey, value, time.Hour).Err(); err != nil {
			log.Printf("ExchangeSyncer: failed set %s: %v", redisKey, err)
			return
		}
		log.Printf("Exchange rate cached in Redis: %s", redisKey)
	})
}
