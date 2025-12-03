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
	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("❌ Redis connection failed: %v", err)
	}
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
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		log.Println("🛑 Shutting down...")
		srv.Shutdown(ctx)
	}()

	log.Printf("🚀 Server started on :%s", port)
	log.Fatal(srv.ListenAndServe())
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
