package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"service-info/internal/messaging"
	"service-info/internal/models"

	"github.com/redis/go-redis/v9"
)

type ExchangeService struct {
	redis    *redis.Client
	producer *messaging.Producer
	http     *http.Client
}

func NewExchangeService(redis *redis.Client, producer *messaging.Producer) *ExchangeService {
	return &ExchangeService{
		redis:    redis,
		producer: producer,
		http:     &http.Client{Timeout: 10 * time.Second},
	}
}

// GetRate — возвращает данные сразу после API, НЕ пишет в Redis
func (s *ExchangeService) GetRate(base, target string) (*models.ExchangeRate, error) {
	ctx := context.Background()
	key := "exchange:" + strings.ToLower(base+"_"+target)

	// 1️⃣ ✅ Redis check
	if data, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var rate models.ExchangeRate
		if json.Unmarshal(data, &rate) == nil {
			log.Printf("✅ Cache HIT: %s", key)
			return &rate, nil
		}
	}

	// 2.1 📡 Запрос к API
	rate, err := s.fetchFromAPI(base, target)
	if err != nil {
		return nil, fmt.Errorf("API error: %w", err)
	}

	// 2.3 🔁 Публикуем в Kafka — асинхронно, без блокировки
	if s.producer != nil {
		go func() {
			kafkaKey := []byte(key)
			kafkaValue, _ := json.Marshal(rate)
			if err := s.producer.Publish(kafkaKey, kafkaValue); err != nil {
				log.Printf("⚠️ Kafka publish failed (exchange): %v", err)
			} else {
				log.Printf("📤 Published to Kafka (exchange): %s", key)
			}
		}()
	}

	// 2.2 ✅ Возвращаем клиенту СРАЗУ
	return rate, nil
}

func (s *ExchangeService) fetchFromAPI(base, target string) (*models.ExchangeRate, error) {
	apiKey := os.Getenv("FREECURRENCY_API_KEY") // лучше из env
	if apiKey == "" {
		return nil, fmt.Errorf("FREECURRENCY_API_KEY not set")
	}

	url := fmt.Sprintf(
		"https://api.freecurrencyapi.com/v1/latest?apikey=%s&base_currency=%s&currencies=%s",
		url.QueryEscape(apiKey),
		url.QueryEscape(base),
		url.QueryEscape(target),
	)

	resp, err := s.http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Data map[string]float64 `json:"data"`
		Meta struct {
			LastUpdated string `json:"last_updated_at"`
		} `json:"meta"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("JSON parse error: %w", err)
	}

	rate, ok := result.Data[strings.ToUpper(target)]
	if !ok {
		return nil, fmt.Errorf("currency %s not found", target)
	}

	return &models.ExchangeRate{
		Base:    base,
		Target:  target,
		Rate:    rate,
		Updated: result.Meta.LastUpdated,
	}, nil
}
