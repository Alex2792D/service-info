package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"service-info/internal/handlers"
	"service-info/internal/messaging"
	"service-info/internal/services"

	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	godotenv.Load()

	// 🔌 Config
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := []string{getEnv("KAFKA_BROKER", "localhost:9092")}
	kafkaTopic := getEnv("KAFKA_TOPIC", "weather-updates")

	// 📡 Redis client (shared: WeatherService reads, Consumer writes)
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()

	// 📡 Producer
	producer := messaging.NewProducer(kafkaBrokers, kafkaTopic)
	defer producer.Close()

	// 📥 Consumer
	consumer := messaging.NewConsumer(kafkaBrokers, kafkaTopic, "weather-redis-syncer", redisClient)
	consumer.Start()
	defer consumer.Stop()

	// 🌤️ Service
	weatherService := services.NewWeatherService(redisClient, producer)
	handler := handlers.NewWeatherHandler(weatherService)

	// 🧭 Router
	r := chi.NewRouter()
	r.Get("/weather", handler.GetWeather)

	// 🏁 Server
	srv := &http.Server{Addr: ":8080", Handler: r}

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		log.Println("🛑 Shutting down...")
		srv.Shutdown(ctx)
	}()

	log.Println("🚀 Server started on :8080")
	log.Fatal(srv.ListenAndServe())
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
