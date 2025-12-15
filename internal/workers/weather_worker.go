package workers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"service-info/internal/api"

	"github.com/redis/go-redis/v9"
)

type WeatherWorker struct {
	messages chan []byte
	redis    *redis.Client
}

func NewWeatherWorker(messages chan []byte, redis *redis.Client) *WeatherWorker {
	return &WeatherWorker{messages: messages, redis: redis}
}

func (w *WeatherWorker) Start(ctx context.Context) {
	log.Println("WeatherWorker started")
	for {
		select {
		case msg := <-w.messages:
			var wrapper struct {
				Type string            `json:"type"`
				Args map[string]string `json:"args"`
			}
			if err := json.Unmarshal(msg, &wrapper); err != nil {
				log.Printf("Failed to unmarshal weather message: %v", err)
				continue
			}

			city := wrapper.Args["city"]
			if city == "" {
				log.Println("WeatherWorker: city empty")
				continue
			}

			weather, err := api.FetchWeather(city)
			if err != nil {
				log.Printf("Failed to fetch weather for %s: %v", city, err)
				continue
			}

			key := "weather:" + city
			data, _ := json.Marshal(weather)
			if err := w.redis.Set(ctx, key, data, 10*time.Minute).Err(); err != nil {
				log.Printf("Failed to set Redis for %s: %v", key, err)
			} else {
				log.Printf("Weather updated in Redis: %s = %.1f°C", key, weather.Temp)
			}

		case <-ctx.Done():
			log.Println("WeatherWorker stopped")
			return
		}
	}
}
