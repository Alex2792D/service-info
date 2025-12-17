// test/integration/user_test.go
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"service-info/internal/handlers"
	"service-info/internal/kafka"
	"service-info/internal/models"
	"service-info/internal/repositories"
	"service-info/internal/services"
	testutils "service-info/test/utils"

	"github.com/avast/retry-go/v4"
	"github.com/redis/go-redis/v9"
)

func TestCreateUser_KafkaToRedis(t *testing.T) {
	// Подготовка тестовой БД
	db := testutils.TestDBWithCleanup(t)

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6380"})
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis недоступен: %v", err)
	}

	// // Создаем Kafka тему
	testutils.CreateKafkaTopic(t, "user-events")
	consumer := kafka.NewConsumer("user-events", "test-group-"+t.Name())
	go StartUserSyncer(redisClient, kafkaBundle.UserConsumer)
	producer := kafka.NewProducer("user-events")

	defer consumer.Stop()

	// Даём немного времени на подключение
	time.Sleep(1 * time.Second)

	t.Log("✅ Consumer инициализирован и запущен")
	// Стартуем syncer

	// Сервисы и handler
	userRepo := repositories.NewUserRepository(db)
	userService := services.NewUserService(userRepo, producer)
	userHandler := handlers.NewUserHandler(userService)

	// Router
	router := http.NewServeMux()
	router.HandleFunc("/users", userHandler.CreateUser)

	srv := httptest.NewServer(router)
	defer srv.Close()

	// Данные пользователя
	userData := models.UserData{
		UserName:  "alex",
		FirstName: "Алекс",
		LastName:  "Тестов",
	}
	payload, _ := json.Marshal(userData)

	// Отправляем запрос
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

	// 🕒 Ждем появления данных в Redis через retry
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var val string
	err = retry.Do(
		func() error {
			var getErr error
			val, getErr = rdb.Get(ctx, "user:999").Result()
			return getErr
		},
		retry.Attempts(50),
		retry.Delay(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("данные не появились в Redis за 5 сек: %v", err)
	}

	// Закрываем producer и consumer
	producer.Close()
	consumer.Stop()

	// Проверка содержимого
	var fromRedis models.UserData
	if err := json.Unmarshal([]byte(val), &fromRedis); err != nil {
		t.Fatalf("не удалось распарсить данные из Redis: %v", err)
	}

	if fromRedis.UserName != "alex" {
		t.Errorf("ожидали UserName=alex, получили %q", fromRedis.UserName)
	}

		t.Log("✅ SUCCESS: Kafka → Redis работает!")
	}

test/integration/user_test.go
