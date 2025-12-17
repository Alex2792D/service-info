package workers

import (
	"context"

	"service-info/internal/kafka"

	"github.com/redis/go-redis/v9"
)

type WorkerBundle struct {
	Workers []Worker
}

func StartAllWorkers(
	ctx context.Context,
	redisClient *redis.Client,
	kafkaBundle *kafka.KafkaBundle,
) *WorkerBundle {

	go StartUserSyncer(redisClient, kafkaBundle.UserConsumer)
	go StartWeatherSyncer(redisClient, kafkaBundle.WeatherConsumer)
	go StartExchangeSyncer(redisClient, kafkaBundle.ExchangeConsumer)

	weatherCh := make(chan []byte, 100)
	exchangeCh := make(chan []byte, 100)

	StartWorkerMultiplexer(kafkaBundle.PopularConsumer, weatherCh, exchangeCh)

	weatherWorker := NewGenericWorker(weatherCh, redisClient, WeatherWorkerHandler{})
	exchangeWorker := NewGenericWorker(exchangeCh, redisClient, ExchangeWorkerHandler{})

	go weatherWorker.Start(ctx)
	go exchangeWorker.Start(ctx)

	return &WorkerBundle{
		Workers: []Worker{
			weatherWorker,
			exchangeWorker,
		},
	}
}
