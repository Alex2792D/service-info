// package main

// import (
// 	"context"
// 	"crypto/tls"
// 	"log"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"service-info/internal/handlers"
// 	"service-info/internal/messaging"
// 	"service-info/internal/services"
// 	"syscall"
// 	"time"

// 	"github.com/go-chi/chi/v5"
// 	"github.com/joho/godotenv"
// 	"github.com/redis/go-redis/v9"
// 	"github.com/segmentio/kafka-go"
// )

// func main() {
// 	godotenv.Load()

// 	// 🔌 Config
// 	redisURL := os.Getenv("REDIS_URL")
// 	if redisURL == "" {
// 		redisURL = getEnv("REDIS_ADDR", "redis://localhost:6379") // старый способ или локалка
// 	}

// 	opt, err := redis.ParseURL(redisURL)
// 	if err != nil {
// 		log.Fatalf("❌ Invalid Redis URL: %v", err)
// 	}
// 	kafkaBrokers := []string{getEnv("KAFKA_BROKERS", "localhost:9092")}
// 	kafkaTopic := getEnv("KAFKA_TOPIC", "weather-updates")

// 	redisClient := redis.NewClient(opt)

// 	// Проверяем подключение с таймаутом (на Render иногда нужно пару секунд)
// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	_, err = redisClient.Ping(ctx).Result()
// 	if err != nil {
// 		log.Fatalf("❌ Redis connection failed: %v", err)
// 	}

// 	log.Println("✅ Redis connected successfully")
// 	defer redisClient.Close()

// 	// 📡 Producer
// 	// Для Redpanda (SASL + SSL)
// 	dialer := &kafka.Dialer{
// 		Timeout:   10 * time.Second,
// 		DualStack: true,
// 		TLS:       &tls.Config{},
// 		SASL:      plain.Auth{Username: os.Getenv("KAFKA_USERNAME"), Password: os.Getenv("KAFKA_PASSWORD")}.AsMechanism(),
// 	}

// 	producer := messaging.NewProducer(kafkaBrokers, kafkaTopic, dialer)
// 	defer producer.Close()

// 	// 📥 Consumer
// 	consumer := messaging.NewConsumer(
// 		kafkaBrokers,
// 		kafkaTopic,
// 		"weather-redis-syncer",
// 		redisClient,
// 		dialer, // ← передаём
// 	)
// 	consumer.Start()
// 	defer consumer.Stop()

// 	// 🌤️ Service
// 	weatherService := services.NewWeatherService(redisClient, producer)
// 	handler := handlers.NewWeatherHandler(weatherService)

// 	// 🧭 Router
// 	r := chi.NewRouter()
// 	r.Get("/weather", handler.GetWeather)

// 	// 🏁 Server
// 	port := os.Getenv("PORT")
// 	if port == "" {
// 		port = "8080"
// 	}
// 	srv := &http.Server{Addr: ":" + port, Handler: r}

// 	// Graceful shutdown
// 	go func() {
// 		sig := make(chan os.Signal, 1)
// 		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
// 		<-sig
// 		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 		defer cancel()
// 		log.Println("🛑 Shutting down...")
// 		srv.Shutdown(ctx)
// 	}()

// 	log.Printf("🚀 Server started on :%s", port)
// 	log.Fatal(srv.ListenAndServe())
// }

//	func getEnv(key, fallback string) string {
//		if v := os.Getenv(key); v != "" {
//			return v
//		}
//		return fallback
//	}
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

	// 🔌 Redis
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("❌ REDIS_URL не задан.")
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("❌ Invalid Redis URL: %v", err)
	}
	redisClient := redis.NewClient(opt)
	defer redisClient.Close()

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf(" Redis connection failed: %v", err)
	}
	log.Println("✅ Redis connected successfully")

	// 📡 Kafka (опционально)
	var producer *messaging.Producer
	var consumer *messaging.Consumer

	if kafkaBrokersStr := os.Getenv("KAFKA_BROKERS"); kafkaBrokersStr != "" {
		kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
		kafkaTopic := getEnv("KAFKA_TOPIC", "weather-updates")
		kafkaUser := os.Getenv("KAFKA_USERNAME")
		kafkaPass := os.Getenv("KAFKA_PASSWORD")

		producer = messaging.NewProducer(kafkaBrokers, kafkaTopic, kafkaUser, kafkaPass)
		defer producer.Close()

		consumer = messaging.NewConsumer(kafkaBrokers, kafkaTopic, "weather-redis-syncer", os.Getenv("REDIS_URL"), kafkaPass)
		consumer.Start(func(key []byte, value []byte) {
			log.Printf("Received message: key=%s, value=%s", string(key), string(value))
			// Здесь можно писать в Redis
			redisClient.Set(context.Background(), string(key), value, 10*time.Minute)
		})
		defer consumer.Stop()

		log.Println("✅ Kafka initialized")
	} else {
		log.Println("🟡 Kafka disabled — using direct Redis writes")
	}

	// 🌤️ Weather service
	weatherService := services.NewWeatherService(redisClient, producer)
	handler := handlers.NewWeatherHandler(weatherService)

	// 🧭 Router
	r := chi.NewRouter()
	r.Get("/weather", handler.GetWeather)

	// 🏁 Server
	port := getEnv("PORT", "8080")
	srv := &http.Server{Addr: ":" + port, Handler: r}

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Println("🛑 Shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
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
