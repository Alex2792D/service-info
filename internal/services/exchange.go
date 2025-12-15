package services

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"service-info/internal/api"
	"service-info/internal/kafka"
	"service-info/internal/models"

	"github.com/redis/go-redis/v9"
)

type ExchangeServiceInterface interface {
	GetRate(base, target string) (*models.ExchangeRate, error)
}

type ExchangeService struct {
	redis    *redis.Client
	producer *kafka.Producer
	http     *http.Client
}

func NewExchangeService(redis *redis.Client, producer *kafka.Producer) *ExchangeService {
	return &ExchangeService{
		redis:    redis,
		producer: producer,
		http:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *ExchangeService) GetRate(base, target string) (*models.ExchangeRate, error) {
	ctx := context.Background()
	key := "exchange:" + strings.ToLower(base+"_"+target)

	if data, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var rate models.ExchangeRate
		if json.Unmarshal(data, &rate) == nil {
			log.Printf("Cache HIT: %s", key)
			return &rate, nil
		}
	}

	rate, err := api.FetchExchangeRate(base, target)
	if err != nil {
		return nil, err
	}

	if s.producer != nil {
		kafkaKey := []byte(key)
		s.producer.PublishObjectAsync(kafkaKey, rate)
	}

	return rate, nil
}
