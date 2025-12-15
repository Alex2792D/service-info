package services

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"service-info/internal/kafka"
	"service-info/internal/models"

	"service-info/internal/api"

	"github.com/redis/go-redis/v9"
)

type WeatherServiceInterface interface {
	GetWeatherByCity(city string) (*models.Weather, error)
}

type WeatherService struct {
	redis    *redis.Client
	producer *kafka.Producer
	http     *http.Client
}

func NewWeatherService(redis *redis.Client, producer *kafka.Producer) *WeatherService {
	return &WeatherService{
		redis:    redis,
		producer: producer,
		http:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *WeatherService) GetWeatherByCity(city string) (*models.Weather, error) {
	ctx := context.Background()
	key := "weather:" + strings.ToLower(strings.TrimSpace(city))

	if data, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var w models.Weather
		if json.Unmarshal(data, &w) == nil {
			log.Printf("Cache HIT: %s", city)
			return &w, nil
		}
	}

	w, err := api.FetchWeather(city)
	if err != nil {
		return nil, err
	}

	if s.producer != nil {
		s.producer.PublishObjectAsync([]byte(key), w)
	}

	return w, nil
}
