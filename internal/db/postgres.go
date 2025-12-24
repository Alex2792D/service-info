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
	var db *sql.DB
	var err error

	// Попытки подключения
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", cfg.DatabaseURL)
		if err != nil {
			log.Printf("Ошибка sql.Open(): %v", err)
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = db.PingContext(ctx)
			if err == nil {
				log.Println("PostgreSQL подключён успешно")
				return db
			}
			log.Printf("Попытка %d: не удалось подключиться к PostgreSQL: %v", i+1, err)
		}

		time.Sleep(3 * time.Second) // Ждём перед новой попыткой
	}

	log.Fatalf("Не удалось подключиться к PostgreSQL после 10 попыток: %v", err)
	return nil
}
