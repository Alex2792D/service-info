package db

import (
	"context"
	"database/sql"
	"log"
	"time"

	"service-info/internal/config"

	_ "github.com/lib/pq"
)

func ConnectPostgres(cfg *config.Config) *sql.DB {
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf(" Ошибка sql.Open(): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Не удалось подключиться к PostgreSQL: %v", err)
	}

	log.Println("PostgreSQL подключён успешно")
	return db
}
