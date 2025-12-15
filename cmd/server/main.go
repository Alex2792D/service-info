package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"service-info/internal/bootstrap"
	"service-info/internal/config"
	"service-info/internal/db"
	"service-info/internal/kafka"
	"service-info/internal/workers"
)

func main() {
	// -----------------------------
	// 1. Конфигурация
	// -----------------------------
	cfg := config.Load()
	log.Printf("DATABASE_URL: %s", cfg.DatabaseURL)
	globalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// -----------------------------
	// 2. Подключения к БД и Redis
	// -----------------------------
	dbConn := db.ConnectPostgres(cfg)
	defer dbConn.Close()

	redisClient := db.ConnectRedis(cfg)
	defer redisClient.Close()

	// -----------------------------
	// 3. Kafka: продюсеры и консумеры
	// -----------------------------
	kafkaBundle := kafka.InitKafka()

	// -----------------------------
	// -----------------------------
	// 4. Воркеры
	// -----------------------------
	ctx := context.Background()
	_ = workers.StartAllWorkers(ctx, redisClient, kafkaBundle)
	// 5. Репозитории, сервисы, хэндлеры
	// -----------------------------
	bundle := bootstrap.InitBootstrap(dbConn, redisClient, kafkaBundle)
	// -----------------------------
	// 6. Cron jobs
	// -----------------------------
	bootstrap.StartCronJobs(globalCtx, bundle.Repositories.AdminRepo, kafkaBundle, cfg.PopularTopic)
	// -----------------------------
	// 7. Router
	// -----------------------------
	r := bootstrap.InitRoutes(
		bundle.Handlers.UserHandler,
		bundle.Handlers.AdminHandler,
		bundle.Handlers.WeatherHandler,
		bundle.Handlers.ExchangeHandler,
		redisClient,
	)

	// -----------------------------
	// 8. Запуск сервера с graceful shutdown
	// -----------------------------
	port := cfg.Port
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	bootstrap.GracefulShutdown(srv, redisClient, kafkaBundle)

	log.Printf("🚀 Server starting on :%s", port)
	log.Fatal(srv.ListenAndServe())
}

// 	redisURL := os.Getenv("REDIS_URL")
// 	if redisURL == "" {
// 		redisURL = "redis://localhost:6379"
// 	}
// 	opt, err := redis.ParseURL(redisURL)
// 	if err != nil {
// 		log.Fatalf("❌ Invalid Redis URL: %v", err)
// 	}
// 	redisClient := redis.NewClient(opt)

// 	ctx = context.Background()
// 	if err := redisClient.Ping(ctx).Err(); err != nil {
// 		log.Fatalf("❌ Redis connection failed: %v", err)
// 	}
// 	log.Println("✅ Redis connected successfully")
// }
// 	// // ------------------------
// // Kafka — раздельные топики
// // ------------------------
// weatherTopic := getEnv("WEATHER_KAFKA_TOPIC", "weather-updates")
// userTopic := getEnv("USER_KAFKA_TOPIC", "user-events")
// exchangeTopic := getEnv("EXCHANGE_KAFKA_TOPIC", "exchange-updates")

// weatherProducer := messaging.NewProducer(weatherTopic)
// userProducer := messaging.NewProducer(userTopic)

// exchangeProducer := messaging.NewProducer(exchangeTopic)

// // Consumer для курса валют→ Redis
// exchangeConsumer := messaging.NewConsumer(exchangeTopic, "exchange-redis-syncer")
// exchangeConsumer.Start(func(key, value []byte) {
// 	keyStr := string(key)
// 	if err := redisClient.Set(ctx, keyStr, value, 1*time.Hour).Err(); err != nil {
// 		log.Printf("❌ Redis exchange write error: %v", err)
// 	} else {
// 		log.Printf("✅ Redis updated (exchange): %s", keyStr)
// 	}
// })
// // Consumer для погоды → Redis
// weatherConsumer := messaging.NewConsumer(weatherTopic, "weather-redis-syncer")
// weatherConsumer.Start(func(key, value []byte) {
// 	var msg map[string]interface{}
// 	if err := json.Unmarshal(value, &msg); err != nil {
// 		log.Printf("❌ Invalid Kafka weather message: %v", err)
// 		return
// 	}

// 	if keyStr := string(key); keyStr != "" {
// 		data, _ := json.Marshal(msg)
// 		if err := redisClient.Set(ctx, keyStr, data, 10*time.Minute).Err(); err != nil {
// 			log.Printf("❌ Redis weather write error: %v", err)
// 		} else {
// 			log.Printf("✅ Redis updated (weather): %s", keyStr)
// 		}
// 	}
// })

// userConsumer := messaging.NewConsumer(userTopic, "user-redis-syncer")
// userConsumer.Start(func(key, value []byte) {
// 	var user map[string]interface{}
// 	if err := json.Unmarshal(value, &user); err != nil {
// 		log.Printf("❌ Invalid Kafka user message: %v", err)
// 		return
// 	}
// 	keyStr := "user:" + string(key)

// 	if err := redisClient.Set(ctx, keyStr, value, 24*time.Hour).Err(); err != nil {
// 		log.Printf("❌ Redis user write error: %v", err)
// 	} else {
// 		log.Printf("✅ Redis updated (user): %s", keyStr)

// 		// Выводим все user-ключи ПОСЛЕ успешной записи
// 		middleware.PrintAllUserKeys(redisClient)
// 	}
// })
