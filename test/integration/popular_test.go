// test/integration/popular_test.go
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"service-info/internal/cron"
	"service-info/internal/handlers"
	"service-info/internal/kafka"
	"service-info/internal/models"
	"service-info/internal/repositories"
	"service-info/internal/services"
	"service-info/internal/workers"
	testutils "service-info/test/utils"

	"github.com/avast/retry-go/v4"
	"github.com/redis/go-redis/v9"
)

func TestPopularPublisher_KafkaToRedis(t *testing.T) {
	log.Println("🧪 Starting TestPopularPublisher_KafkaToRedis...")

	db := testutils.TestDBWithCleanup(t)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("❌ Redis недоступен: %v", err)
	}
	log.Println("✅ Redis connected")

	testutils.CreateKafkaTopic(t, "popular-requests")
	time.Sleep(500 * time.Millisecond)
	log.Println("✅ Kafka topic 'popular-requests' created")

	producer := kafka.NewProducer("popular-requests")
	defer producer.Close()

	consumer := kafka.NewConsumer("popular-requests", "test-popular-"+t.Name())
	log.Println("✅ Kafka producer & consumer ready")

	weatherCh := make(chan []byte, 100)
	exchangeCh := make(chan []byte, 100)

	go workers.StartWorkerMultiplexer(consumer, weatherCh, exchangeCh)
	log.Println("✅ WorkerMultiplexer started")

	weatherWorker := workers.NewGenericWorker(
		weatherCh, rdb, workers.WeatherWorkerHandler{},
	)
	exchangeWorker := workers.NewGenericWorker(
		exchangeCh, rdb, workers.ExchangeWorkerHandler{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go weatherWorker.Start(ctx)
	go exchangeWorker.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	log.Println("✅ Generic workers started")

	adminRepo := repositories.NewAdminRepository(db)
	adminService := services.NewAdminService(adminRepo)
	adminHandler := handlers.NewAdminHandler(adminService)

	router := http.NewServeMux()
	router.HandleFunc("POST /admin/popular", adminHandler.CreatePopular)
	srv := httptest.NewServer(router)
	defer srv.Close()

	payload := `{"text":"/weather Moscow"}`
	req, _ := http.NewRequest("POST", srv.URL+"/admin/popular", bytes.NewReader([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("❌ HTTP request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("❌ Expected 201, got %d", resp.StatusCode)
	}
	log.Println("✅ '/weather Moscow' saved to DB")

	publisher := cron.NewPopularPublisher(
		adminRepo,
		producer,
		"popular-requests",
		1*time.Minute,
	)

	if err := publisher.RunOnce(context.Background()); err != nil {
		t.Fatalf("❌ PopularPublisher.RunOnce() failed: %v", err)
	}
	log.Println("✅ PopularPublisher.RunOnce() executed")

	err = retry.Do(
		func() error {
			return rdb.Get(context.Background(), "weather:moscow").Err()
		},
		retry.Attempts(50),
		retry.Delay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("❌ Key 'weather:moscow' not in Redis after 5s: %v", err)
	}

	var fromRedis models.Weather
	val, _ := rdb.Get(context.Background(), "weather:moscow").Result()
	if err := json.Unmarshal([]byte(val), &fromRedis); err != nil {
		t.Fatalf("❌ JSON unmarshal failed: %v", err)
	}

	if fromRedis.City != "Moscow" {
		t.Errorf("❌ Expected City='Moscow', got %q", fromRedis.City)
	} else {
		log.Printf("✅ Weather for %q received", fromRedis.City)
	}

	log.Println("🎉 TEST PASSED: PopularPublisher → Kafka → Multiplexer → Worker → Redis")
}
