// package workers

// import (
// 	"context"
// 	"strings"

// 	"service-info/internal/api"
// 	"service-info/internal/models"
// )

// type WeatherWorkerHandler struct{}

// func (WeatherWorkerHandler) Type() string {
// 	return "weather"
// }

// func (WeatherWorkerHandler) Handle(
// 	ctx context.Context,
// 	args map[string]string,
// ) (*models.Weather, string, error) {

// 	city := strings.TrimSpace(args["city"])
// 	weather, err := api.FetchWeather(city)
// 	if err != nil {
// 		return nil, "", err
// 	}

// 	key := "weather:" + strings.ToLower(city)
// 	return weather, key, nil
// }

// func (WeatherWorkerHandler) TTL() int {
// 	return 600
// }

// workers/weather_worker_handler.go
package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"service-info/internal/api"
	"service-info/internal/models"
)

type WeatherWorkerHandler struct{}

func (WeatherWorkerHandler) Type() string {
	return "weather"
}

// Handle поддерживает:
//   - команды: {"type":"weather","args":{"city":"Moscow"}}
//   - готовые объекты: {"city":"Moscow","temp":5.2,...}
func (h WeatherWorkerHandler) Handle(
	ctx context.Context,
	key, value []byte,
) (*models.Weather, string, error) {

	var cmd struct {
		Type string            `json:"type"`
		Args map[string]string `json:"args"`
	}
	if err := json.Unmarshal(value, &cmd); err == nil && cmd.Type == "weather" {
		city := strings.TrimSpace(cmd.Args["city"])
		if city == "" {
			return nil, "", fmt.Errorf("city is required in command")
		}
		weather, err := api.FetchWeather(city)
		if err != nil {
			return nil, "", err
		}
		cacheKey := "weather:" + strings.ToLower(city)
		return weather, cacheKey, nil
	}

	var weather models.Weather
	if err := json.Unmarshal(value, &weather); err != nil {
		return nil, "", fmt.Errorf("invalid weather JSON: %w", err)
	}
	if weather.City == "" {
		return nil, "", fmt.Errorf("city is empty in weather object")
	}
	cacheKey := "weather:" + strings.ToLower(weather.City)
	return &weather, cacheKey, nil
}

func (WeatherWorkerHandler) TTL() int {
	return 600
}
