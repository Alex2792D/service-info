package services

import (
	"strings"

	"service-info/internal/api"
	"service-info/internal/models"
)

type WeatherFetcher struct{}

func (WeatherFetcher) CacheKey(params ...string) string {
	city := strings.ToLower(strings.TrimSpace(params[0]))
	return "weather:" + city
}

func (WeatherFetcher) Fetch(params ...string) (*models.Weather, error) {
	return api.FetchWeather(params[0])
}
