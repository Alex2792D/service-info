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
	"encoding/json"
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
	// Загружаем .env
	godotenv.Load()

	// ------------------------
	// Redis
	// ------------------------
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("❌ Invalid Redis URL: %v", err)
	}
	redisClient := redis.NewClient(opt)
	defer redisClient.Close()

	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis connection failed: %v", err)
	}
	log.Println("✅ Redis connected successfully")

	// ------------------------
	// Kafka
	// ------------------------
	var producer *messaging.Producer
	var consumer *messaging.Consumer

	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "weather-updates")

	producer = messaging.NewProducer(kafkaBrokers, kafkaTopic)
	consumer = messaging.NewConsumer(kafkaBrokers, kafkaTopic, "weather-redis-syncer", redisClient)

	// Consumer с обработчиком сообщений
	consumer.Start(func(key, value []byte) {
		var msg map[string]interface{}
		if err := json.Unmarshal(value, &msg); err != nil {
			log.Printf("❌ Invalid Kafka message: %v", err)
			return
		}

		// Сохраняем в Redis
		if keyStr := string(key); keyStr != "" {
			data, _ := json.Marshal(msg)
			if err := redisClient.Set(ctx, keyStr, data, 10*time.Minute).Err(); err != nil {
				log.Printf("❌ Redis write error: %v", err)
			} else {
				log.Printf("✅ Redis updated for key: %s", keyStr)
			}
		}
	})
	defer consumer.Stop()
	defer producer.Close()

	log.Println("✅ Kafka producer/consumer initialized")

	// ------------------------
	// Weather Service + Handler
	// ------------------------
	weatherService := services.NewWeatherService(redisClient, producer)
	handler := handlers.NewWeatherHandler(weatherService)

	// ------------------------
	// Router
	// ------------------------
	r := chi.NewRouter()
	r.Get("/weather", handler.GetWeather)

	// ------------------------
	// Server
	// ------------------------
	port := getEnv("PORT", "8080")
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

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
