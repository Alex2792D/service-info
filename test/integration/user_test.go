// test/integration/user_test.go
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

func TestCreateUser_KafkaToRedis(t *testing.T) {

	db := testutils.TestDBWithCleanup(t)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis недоступен: %v", err)
	}

	testutils.CreateKafkaTopic(t, "user-events")
	time.Sleep(500 * time.Millisecond)

	consumer := kafka.NewConsumer("user-events", "test-group-"+t.Name())
	// defer consumer.Stop()
	go workers.StartUserSyncer(rdb, consumer)
	time.Sleep(1 * time.Second)

	log.Println("✅ Consumer инициализирован и запущен")

	producer := kafka.NewProducer("user-events")
	defer producer.Close()

	userRepo := repositories.NewUserRepository(db)
	userService := services.NewUserService(userRepo, producer)
	userHandler := handlers.NewUserHandler(userService)

	router := http.NewServeMux()
	router.HandleFunc("/users", userHandler.CreateUser)
	srv := httptest.NewServer(router)
	defer srv.Close()

	userData := models.UserData{
		UserName:  "alex",
		FirstName: "Алекс",
		LastName:  "Тестов",
	}
	payload, _ := json.Marshal(userData)

	req, _ := http.NewRequest("POST", srv.URL+"/users", bytes.NewReader(payload))
	req.Header.Set("X-User-ID", "999")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("ожидали 201, получили %d", resp.StatusCode)
	}

	log.Println("✅ HTTP запрос выполнен успешно")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var redisVal string
	err = retry.Do(
		func() error {
			var getErr error
			redisVal, getErr = rdb.Get(ctx, "user:999").Result()
			return getErr
		},
		retry.Attempts(150),
		retry.Delay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("данные не появились в Redis за 15 секунд: %v", err)
	}

	var fromRedis models.UserData
	if err := json.Unmarshal([]byte(redisVal), &fromRedis); err != nil {
		t.Fatalf("не удалось распарсить данные из Redis: %v", err)
	}

	if fromRedis.UserName != "alex" {
		t.Errorf("ожидали UserName=alex, получили %q", fromRedis.UserName)
	}

	log.Println("✅ SUCCESS: Kafka → Redis работает!")
}
