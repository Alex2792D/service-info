// test/integration/weather_test.go
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

func TestGetWeather_KafkaToRedis(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis недоступен: %v", err)
	}

	testutils.CreateKafkaTopic(t, "weather-updates")
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := kafka.NewConsumer(
		"weather-updates",
		"test-weather-"+t.Name(),
	)

	workers.StartAllWorkers(
		ctx,
		rdb,
		&kafka.KafkaBundle{
			WeatherConsumer: consumer,
		},
	)

	time.Sleep(1 * time.Second)
	log.Println("✅ WeatherWorker запущен")

	producer := kafka.NewProducer("weather-updates")
	defer producer.Close()

	weatherService := services.NewCacheService[models.Weather](
		rdb,
		producer,
		services.WeatherFetcher{},
	)

	weatherHandler := handlers.NewWeatherHandler(weatherService)

	router := http.NewServeMux()
	router.HandleFunc("/weather", weatherHandler.GetWeather)
	srv := httptest.NewServer(router)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/weather?city=Moscow", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Ожидали 200, получили %d", resp.StatusCode)
	}

	log.Println("✅ HTTP запрос выполнен успешно")

	defer cancel()

	var redisVal string
	err = retry.Do(
		func() error {
			return rdb.Get(ctx, "weather:moscow").Scan(&redisVal)
		},
		retry.Attempts(150),
		retry.Delay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Данные не в Redis за 15 сек: %v", err)
	}

	var fromRedis models.Weather
	if err := json.Unmarshal([]byte(redisVal), &fromRedis); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}

	if fromRedis.Temp == 0 {
		t.Error("Temp == 0 — возможно, нет WEATHERAPI_KEY или ошибка API")
	}
	if fromRedis.City == "" {
		t.Error("City пустой")
	}

	log.Println("✅ SUCCESS: Weather — Kafka → Redis работает!")
}
