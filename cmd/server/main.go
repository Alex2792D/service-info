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
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
	log.Println("✅ Server stopped")
}
