// test/integration/utils/db.go
package testutils

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func TestDBWithCleanup(t *testing.T) *sql.DB {
	t.Helper()

	dsn := "host=localhost port=5433 user=testuser password=testpass dbname=postgres sslmode=disable"
	if envDSN := os.Getenv("TEST_PG_ADMIN_URL"); envDSN != "" {
		dsn = envDSN
	}

	adminDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("❌ Не удалось открыть admin-подключение: %v", err)
	}
	defer adminDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := adminDB.PingContext(ctx); err != nil {
		t.Fatalf("❌ Admin-БД недоступна: %v", err)
	}

	_, err = adminDB.ExecContext(ctx, "CREATE DATABASE testapp_test")
	if err != nil && !isDuplicateDBError(err) {
		t.Fatalf("❌ Не удалось создать БД testapp_test: %v", err)
	}

	appDSN := "host=localhost port=5433 user=testuser password=testpass dbname=testapp_test sslmode=disable"
	if envDSN := os.Getenv("TEST_DATABASE_URL"); envDSN != "" {
		appDSN = envDSN
	}

	db, err := sql.Open("postgres", appDSN)
	if err != nil {
		t.Fatalf("❌ Не удалось открыть подключение к testapp_test: %v", err)
	}

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("❌ БД testapp_test недоступна: %v", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	t.Cleanup(func() {
		db.Close()
		_, _ = adminDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS testapp_test WITH (FORCE)")
	})

	setupSchema(db, t)
	return db
}

func setupSchema(db *sql.DB, t *testing.T) {
	t.Helper()

	_, err := db.Exec(`
		DROP TABLE IF EXISTS users CASCADE;
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			user_id BIGINT NOT NULL UNIQUE,
			username TEXT NOT NULL,
			first_name TEXT,
			last_name TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
		DROP TABLE IF EXISTS scheduled_tasks CASCADE;
		CREATE TABLE scheduled_tasks (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			args JSONB NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		);
	`)
	if err != nil {
		t.Fatalf("❌ Ошибка создания схемы: %v", err)
	}
}

func isDuplicateDBError(err error) bool {
	return err != nil &&
		(err.Error() == `pq: database "testapp_test" already exists` ||
			err.Error() == `ERROR: database "testapp_test" already exists (SQLSTATE 42P04)`)
}
