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

	middleware "service-info/internal/Middleware"
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
	defer db.Close()

	// ------------------------
	// ✅ СЕРЫЙ БЛОК: СОЗДАНИЕ ТАБЛИЦЫ И ИНДЕКСА
	// ------------------------
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
	// ------------------------

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
	exchangeTopic := getEnv("EXCHANGE_KAFKA_TOPIC", "exchange-updates")

	weatherProducer := messaging.NewProducer(weatherTopic)
	userProducer := messaging.NewProducer(userTopic)

	exchangeProducer := messaging.NewProducer(exchangeTopic)

	// Consumer для курса валют→ Redis
	exchangeConsumer := messaging.NewConsumer(exchangeTopic, "exchange-redis-syncer")
	exchangeConsumer.Start(func(key, value []byte) {
		keyStr := string(key)
		// Сохраняем ВСЁ, что пришло от API — как есть (как в weatherConsumer)
		if err := redisClient.Set(ctx, keyStr, value, 1*time.Hour).Err(); err != nil {
			log.Printf("❌ Redis exchange write error: %v", err)
		} else {
			log.Printf("✅ Redis updated (exchange): %s", keyStr)
		}
	})
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

	// 🔁 Consumer для пользователей → Redis
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
	userService := services.NewUserService(userRepo, userProducer)
	handler := handlers.NewWeatherHandler(weatherService)
	handlerUser := handlers.NewUserHandler(userService)

	exchangeService := services.NewExchangeService(redisClient, exchangeProducer)
	exchangeHandler := handlers.NewExchangeHandler(exchangeService)
	// ------------------------
	// Router
	// ------------------------
	r := chi.NewRouter()

	// 🟢 КРИТИЧЕСКАЯ ПРАВКА №1: ОБЯЗАТЕЛЬНЫЙ HEALTH-CHECK
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))

	})
	// В main.go после создания роутера:

	// Публичные роуты — доступны всем
	r.Post("/user", handlerUser.CreateUser) // сюда приходит sendUserData

	// Защищённые роуты — только для авторизованных
	r.Group(func(r chi.Router) {
		r.Use(middleware.AuthRequired(redisClient)) // ← middleware здесь
		r.Get("/weather", handler.GetWeather)
		r.Get("/exchange", exchangeHandler.GetRate) // ← новая ручка
		// r.Post("/user", handlerUser.CreateUser)

	})

	// ------------------------
	// Server
	// ------------------------
	port := getEnv("PORT", "8080")
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// ------------------------
	// ✅ СЕРЫЙ БЛОК: GRACEFUL SHUTDOWN — НЕ ТРОГАТЬ (оставить как есть)
	// ------------------------
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig

		log.Println("🛑 Shutting down gracefully...")

		weatherConsumer.Stop()
		userConsumer.Stop()
		weatherProducer.Close()
		userProducer.Close()

		if err := redisClient.Close(); err != nil {
			log.Printf("⚠️ Redis close error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("⚠️ Server shutdown error: %v", err)
		}
	}()
	// ------------------------

	// 🟢 КРИТИЧЕСКАЯ ПРАВКА №2: ЗАПУСК СЕРВЕРА — ДОЛЖЕН БЫТЬ ПОСЛЕДНИМ И БЛОКИРУЮЩИМ
	log.Printf("🚀 Server starting on :%s", port)
	log.Fatal(srv.ListenAndServe()) // ← ЭТО ПОСЛЕДНЯЯ СТРОКА main()
}

// ← сюда ничего не добавлять

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
