// package workers

// import (
// 	"context"

// 	"service-info/internal/kafka"

// 	"github.com/redis/go-redis/v9"
// )

// type WorkerBundle struct {
// 	Workers []Worker
// }

// func StartAllWorkers(
// 	ctx context.Context,
// 	redisClient *redis.Client,
// 	kafkaBundle *kafka.KafkaBundle,
// ) *WorkerBundle {

// 	go StartUserSyncer(redisClient, kafkaBundle.UserConsumer)
// 	go StartWeatherSyncer(redisClient, kafkaBundle.WeatherConsumer)
// 	go StartExchangeSyncer(redisClient, kafkaBundle.ExchangeConsumer)

// 	weatherCh := make(chan []byte, 100)
// 	exchangeCh := make(chan []byte, 100)

// 	StartWorkerMultiplexer(kafkaBundle.PopularConsumer, weatherCh, exchangeCh)

// 	weatherWorker := NewGenericWorker(weatherCh, redisClient, WeatherWorkerHandler{})
// 	exchangeWorker := NewGenericWorker(exchangeCh, redisClient, ExchangeWorkerHandler{})

// 	go weatherWorker.Start(ctx)
// 	go exchangeWorker.Start(ctx)

// 	return &WorkerBundle{
// 		Workers: []Worker{
// 			weatherWorker,
// 			exchangeWorker,
// 		},
// 	}
// }

// workers/worker_bundle.go
package workers

import (
	"context"
	"log"

	"service-info/internal/kafka"

	"github.com/redis/go-redis/v9"
)

type WorkerBundle struct {
	Workers []Worker
}

func singleConsumerToChannels(consumer *kafka.Consumer, chs ...chan []byte) {
	if consumer == nil {
		return
	}
	consumer.Start(func(key, value []byte) {
		for _, ch := range chs {
			if ch != nil {
				select {
				case ch <- value:
				default:
					log.Printf("⚠️ Channel full, dropping message for %v", ch)
				}
			}
		}
	})
}

func StartAllWorkers(
	ctx context.Context,
	redisClient *redis.Client,
	kafkaBundle *kafka.KafkaBundle,
) *WorkerBundle {

	weatherCh := make(chan []byte, 100)
	exchangeCh := make(chan []byte, 100)

	singleConsumerToChannels(kafkaBundle.WeatherConsumer, weatherCh)
	singleConsumerToChannels(kafkaBundle.ExchangeConsumer, exchangeCh)
	singleConsumerToChannels(kafkaBundle.PopularConsumer, weatherCh, exchangeCh)
	go StartUserSyncer(redisClient, kafkaBundle.UserConsumer)

	weatherWorker := NewGenericWorker(weatherCh, redisClient, WeatherWorkerHandler{})
	exchangeWorker := NewGenericWorker(exchangeCh, redisClient, ExchangeWorkerHandler{})

	go weatherWorker.Start(ctx)
	go exchangeWorker.Start(ctx)

	return &WorkerBundle{
		Workers: []Worker{weatherWorker, exchangeWorker},
	}
}
