// package main

// import (
// 	"context"
// 	"database/sql"
// 	"encoding/json"
// 	"log"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"syscall"
// 	"time"

// 	"service-info/internal/handlers"
// 	"service-info/internal/messaging"
// 	"service-info/internal/repositories"
// 	"service-info/internal/services"

// 	"github.com/go-chi/chi/v5"
// 	"github.com/joho/godotenv"
// 	_ "github.com/lib/pq"
// 	"github.com/redis/go-redis/v9"
// )

// func main() {
// 	// Загружаем .env
// 	godotenv.Load()

// 	// 1. Подключаемся к PostgreSQL
// 	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=pass dbname=users sslmode=disable")
// 	if err != nil {
// 		log.Fatal("❌ Не удалось создать соединение с БД:", err)
// 	}
// 	defer db.Close()

// 	// 2. Проверяем подключение с таймаутом
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()
// 	if err := db.PingContext(ctx); err != nil {
// 		log.Fatal("❌ Не удалось подключиться к БД:", err)
// 	}
// 	log.Println("✅ Успешное подключение к PostgreSQL")

// 	// ------------------------
// 	// Redis
// 	// ------------------------
// 	redisURL := os.Getenv("REDIS_URL")
// 	if redisURL == "" {
// 		redisURL = "redis://localhost:6379"
// 	}
// 	opt, err := redis.ParseURL(redisURL)
// 	if err != nil {
// 		log.Fatalf("❌ Invalid Redis URL: %v", err)
// 	}
// 	redisClient := redis.NewClient(opt)
// 	defer redisClient.Close()

// 	ctx := context.Background()
// 	if err := redisClient.Ping(ctx).Err(); err != nil {
// 		log.Fatalf("❌ Redis connection failed: %v", err)
// 	}
// 	log.Println("✅ Redis connected successfully")

// 	// ------------------------
// 	// Kafka
// 	// ------------------------
// 	kafkaTopic := getEnv("KAFKA_TOPIC", "weather-updates")
// 	producer := messaging.NewProducer(kafkaTopic)
// 	consumer := messaging.NewConsumer(kafkaTopic, "weather-redis-syncer")

// 	// Consumer с обработчиком сообщений
// 	consumer.Start(func(key, value []byte) {
// 		var msg map[string]interface{}
// 		if err := json.Unmarshal(value, &msg); err != nil {
// 			log.Printf("❌ Invalid Kafka message: %v", err)
// 			return
// 		}

// 		// Сохраняем в Redis
// 		if keyStr := string(key); keyStr != "" {
// 			data, _ := json.Marshal(msg)
// 			if err := redisClient.Set(ctx, keyStr, data, 10*time.Minute).Err(); err != nil {
// 				log.Printf("❌ Redis write error: %v", err)
// 			} else {
// 				log.Printf("✅ Redis updated for key: %s", keyStr)
// 			}
// 		}
// 	})
// 	defer consumer.Stop()
// 	defer producer.Close()

// 	log.Println("✅ Kafka producer/consumer initialized")

// 	// ------------------------
// 	// Weather Service + Handler
// 	// ------------------------
// 	userRepo := repositories.NewUserRepository(db)
// 	weatherService := services.NewWeatherService(redisClient, producer)
// 	userService := services.NewUserService(userRepo, producer)
// 	handler := handlers.NewWeatherHandler(weatherService)
// 	handlerUser := handlers.NewUserHandler(userService)
// 	// ------------------------
// 	// Router
// 	// ------------------------
// 	r := chi.NewRouter()
// 	r.Get("/weather", handler.GetWeather)
// 	r.Post("/user", handlerUser.CreateUser)
// 	// ------------------------
// 	// Server
// 	// ------------------------
// 	port := getEnv("PORT", "8080")
// 	srv := &http.Server{
// 		Addr:    ":" + port,
// 		Handler: r,
// 	}

// 	// Graceful shutdown
// 	go func() {
// 		sig := make(chan os.Signal, 1)
// 		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
// 		<-sig
// 		log.Println("🛑 Shutting down...")
// 		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 		defer cancel()
// 		srv.Shutdown(ctx)
// 	}()

// 	log.Printf("🚀 Server started on :%s", port)
// 	log.Fatal(srv.ListenAndServe())
// }

// func getEnv(key, fallback string) string {
// 	if v := os.Getenv(key); v != "" {
// 		return v
// 	}
// 	return fallback
// }

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"service-info/internal/handlers"
	"service-info/internal/messaging"
	"service-info/internal/repositories"
	"service-info/internal/services"

	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	// Загружаем .env
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️ .env not loaded (ok for prod)")
	}

	// 1. Подключаемся к PostgreSQL
	// Получаем URL из env, fallback — локальный
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("❌ DATABASE_URL is required in production")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal("❌ Не удалось создать соединение с БД:", err)
	}
	_, err = db.Exec(`
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL UNIQUE,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
`)
	if err != nil {
		log.Fatal("❌ Failed to create users table:", err)
	}

	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id);`)
	if err != nil {
		log.Fatal("❌ Failed to create index:", err)
	}

	log.Println("✅ DB schema is up to date")
	defer db.Close()

	// Проверяем подключение
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		log.Fatal("❌ Не удалось подключиться к БД:", err)
	}
	cancel()
	log.Println("✅ Подключение к PostgreSQL установлено")
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

	ctx = context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("❌ Redis connection failed: %v", err)
	}
	log.Println("✅ Redis connected successfully")

	// ------------------------
	// Kafka — раздельные топики
	// ------------------------
	weatherTopic := getEnv("WEATHER_KAFKA_TOPIC", "weather-updates")
	userTopic := getEnv("USER_KAFKA_TOPIC", "user-events")

	weatherProducer := messaging.NewProducer(weatherTopic)
	userProducer := messaging.NewProducer(userTopic)

	// Consumer для погоды → Redis
	weatherConsumer := messaging.NewConsumer(weatherTopic, "weather-redis-syncer")
	weatherConsumer.Start(func(key, value []byte) {
		var msg map[string]interface{}
		if err := json.Unmarshal(value, &msg); err != nil {
			log.Printf("❌ Invalid Kafka weather message: %v", err)
			return
		}

		if keyStr := string(key); keyStr != "" {
			data, _ := json.Marshal(msg)
			if err := redisClient.Set(ctx, keyStr, data, 10*time.Minute).Err(); err != nil {
				log.Printf("❌ Redis weather write error: %v", err)
			} else {
				log.Printf("✅ Redis updated (weather): %s", keyStr)
			}
		}
	})

	// 🔁 Опционально: consumer для пользователей → Redis
	// Раскомментируй, если хочешь синхронизировать пользователей в Redis

	userConsumer := messaging.NewConsumer(userTopic, "user-redis-syncer")
	userConsumer.Start(func(key, value []byte) {
		var user map[string]interface{}
		if err := json.Unmarshal(value, &user); err != nil {
			log.Printf("❌ Invalid Kafka user message: %v", err)
			return
		}
		keyStr := "user:" + string(key)
		if err := redisClient.Set(ctx, keyStr, value, 24*time.Hour).Err(); err != nil {
			log.Printf("❌ Redis user write error: %v", err)
		} else {
			log.Printf("✅ Redis updated (user): %s", keyStr)
		}
	})

	log.Println("✅ Kafka producers/consumers initialized")

	// ------------------------
	// Services & Handlers
	// ------------------------
	userRepo := repositories.NewUserRepository(db)
	weatherService := services.NewWeatherService(redisClient, weatherProducer)
	userService := services.NewUserService(userRepo, userProducer) // ← userProducer!
	handler := handlers.NewWeatherHandler(weatherService)
	handlerUser := handlers.NewUserHandler(userService)

	// ------------------------
	// Router
	// ------------------------
	r := chi.NewRouter()
	r.Get("/weather", handler.GetWeather)
	r.Post("/user", handlerUser.CreateUser)

	// ------------------------
	// Server
	// ------------------------
	port := getEnv("PORT", "8080")
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// Graceful shutdown — управляется вручную
	shutdown := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig

		log.Println("🛑 Shutting down gracefully...")

		// 1. Останавливаем consumer'ов
		weatherConsumer.Stop()
		// userConsumer.Stop() // ← раскомментируй, если включил userConsumer выше

		// 2. Закрываем producer'ов
		weatherProducer.Close()
		userProducer.Close()

		// 3. Закрываем Redis
		if err := redisClient.Close(); err != nil {
			log.Printf("⚠️ Redis close error: %v", err)
		}

		// 4. Останавливаем HTTP-сервер
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("⚠️ Server shutdown error: %v", err)
		}

		close(shutdown)
	}()

	log.Printf("🚀 Server started on :%s", port)

	// Запускаем сервер
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("❌ Server failed: %v", err)
	}

	// Ждём завершения shutdown
	<-shutdown
	log.Println("✅ Server stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
