// test/integration/exchange_test.go
package integration

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"service-info/internal/handlers"
	"service-info/internal/kafka"
	"service-info/internal/models"
	"service-info/internal/services"
	"service-info/internal/workers"
	testutils "service-info/test/utils"

	"github.com/avast/retry-go/v4"
	"github.com/redis/go-redis/v9"
)

func TestGetExchangeRate_KafkaToRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis недоступен: %v", err)
	}

	testutils.CreateKafkaTopic(t, "exchange-updates")
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := kafka.NewConsumer(
		"exchange-updates",
		"test-exchange-"+t.Name(),
	)

	workers.StartAllWorkers(
		ctx,
		rdb,
		&kafka.KafkaBundle{
			ExchangeConsumer: consumer,
		},
	)

	time.Sleep(1 * time.Second)
	log.Println("✅ ExchangeWorker запущен")

	producer := kafka.NewProducer("exchange-updates")
	defer producer.Close()

	exchangeService := services.NewCacheService[models.ExchangeRate](
		rdb,
		producer,
		services.ExchangeFetcher{},
	)

	exchangeHandler := handlers.NewExchangeHandler(exchangeService)

	router := http.NewServeMux()
	router.HandleFunc("/exchange", exchangeHandler.GetRate)
	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequest(
		"GET",
		srv.URL+"/exchange?base=USD&target=EUR",
		nil,
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Ожидали 200, получили %d", resp.StatusCode)
	}

	ctxWait, cancelWait := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelWait()

	var redisVal string
	err = retry.Do(
		func() error {
			return rdb.Get(ctxWait, "exchange:usd_eur").Scan(&redisVal)
		},
		retry.Attempts(150),
		retry.Delay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Данные не в Redis: %v", err)
	}

	var fromRedis models.ExchangeRate
	if err := json.Unmarshal([]byte(redisVal), &fromRedis); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}

	if fromRedis.Base != "USD" {
		t.Errorf("Base != USD")
	}
	if fromRedis.Target != "EUR" {
		t.Errorf("Target != EUR")
	}
	if fromRedis.Rate == 0 {
		t.Error("Rate == 0")
	}

	log.Println("✅ SUCCESS: Exchange — Kafka → Redis работает!")
}
