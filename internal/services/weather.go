package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

type WeatherService struct {
	redis    *redis.Client
	producer *messaging.Producer
	http     *http.Client
}

func NewWeatherService(redis *redis.Client, producer *messaging.Producer) *WeatherService {
	return &WeatherService{
		redis:    redis,
		producer: producer,
		http:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *WeatherService) GetWeatherByCity(city string) (*models.Weather, error) {
	ctx := context.Background()
	key := "weather:" + strings.ToLower(strings.TrimSpace(city))

	// 🔍 Чтение из Redis
	if data, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var w models.Weather
		if json.Unmarshal(data, &w) == nil {
			log.Printf("✅ Cache HIT: %s", city)
			return &w, nil
		}
	}

	// 🌐 Запрос к WeatherAPI
	w, err := s.fetchFromWeatherAPI(city)
	if err != nil {
		return nil, fmt.Errorf("WeatherAPI error: %w", err)
	}

	// 📤 Публикация в Kafka
	if s.producer != nil {
		keyBytes := []byte(key)          // key = "weather:<city>"
		valueBytes, _ := json.Marshal(w) // сериализация Weather
		if err := s.producer.Publish(keyBytes, valueBytes); err != nil {
			log.Printf("❌ Failed to publish weather: %v", err)
		}
	}

	return w, nil
}

func (s *WeatherService) fetchFromWeatherAPI(city string) (*models.Weather, error) {
	apiKey := os.Getenv("WEATHERAPI_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("WEATHERAPI_KEY not set")
	}

	encodedCity := url.QueryEscape(city)
	url := fmt.Sprintf("https://api.weatherapi.com/v1/current.json?key=%s&q=%s&lang=ru", apiKey, encodedCity)

	resp, err := s.http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(body, &errResp)
		return nil, fmt.Errorf("WeatherAPI %d: %s", resp.StatusCode, errResp.Message)
	}

	var apiResp struct {
		Location struct {
			Name string `json:"name"`
		} `json:"location"`
		Current struct {
			TempC     float64 `json:"temp_c"`
			FeelsLike float64 `json:"feelslike_c"`
			Humidity  int     `json:"humidity"`
			Condition struct {
				Text string `json:"text"`
			} `json:"condition"`
			WindKPH      float64 `json:"wind_kph"`
			PressureMB   float64 `json:"pressure_mb"`
			Cloud        int     `json:"cloud"`
			VisibilityKM float64 `json:"vis_km"`
		} `json:"current"`
	}

	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("неверный формат ответа: %w", err)
	}

	return &models.Weather{
		City:         apiResp.Location.Name,
		Temp:         apiResp.Current.TempC,
		FeelsLike:    apiResp.Current.FeelsLike,
		Humidity:     apiResp.Current.Humidity,
		Condition:    apiResp.Current.Condition.Text,
		WindKPH:      apiResp.Current.WindKPH,
		PressureMB:   apiResp.Current.PressureMB,
		Cloud:        apiResp.Current.Cloud,
		VisibilityKM: apiResp.Current.VisibilityKM,
		Updated:      time.Now(),
	}, nil
}
