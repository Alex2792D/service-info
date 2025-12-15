package bootstrap

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"service-info/internal/kafka"

	"github.com/redis/go-redis/v9"
)

func GracefulShutdown(srv *http.Server, redisClient *redis.Client, kafkaBundle *kafka.KafkaBundle) {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig

		log.Println("Shutting down gracefully...")

		if kafkaBundle != nil {
			kafkaBundle.WeatherConsumer.Stop()
			kafkaBundle.UserConsumer.Stop()
			kafkaBundle.ExchangeConsumer.Stop()
			kafkaBundle.PopularConsumer.Stop()

			kafkaBundle.WeatherProducer.Close()
			kafkaBundle.UserProducer.Close()
			kafkaBundle.ExchangeProducer.Close()
		}

		if redisClient != nil {
			if err := redisClient.Close(); err != nil {
				log.Printf("Redis close error: %v", err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf(" Server shutdown error: %v", err)
		}
	}()
}
