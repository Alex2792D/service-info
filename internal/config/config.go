package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DatabaseURL     string
	RedisURL        string
	WeatherTopic    string
	UserTopic       string
	ExchangeTopic   string
	PopularTopic    string
	WeatherAPIKey   string
	FreeCurrencyKey string
	Port            string
}

func Load() *Config {
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Println(".env not loaded (ok for prod)")
	}
	return &Config{
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		RedisURL:        os.Getenv("REDIS_URL"),
		WeatherTopic:    getEnv("WEATHER_KAFKA_TOPIC", "weather-updates"),
		UserTopic:       getEnv("USER_KAFKA_TOPIC", "user-events"),
		ExchangeTopic:   getEnv("EXCHANGE_KAFKA_TOPIC", "exchange-updates"),
		PopularTopic:    "popular-requests",
		WeatherAPIKey:   os.Getenv("WEATHERAPI_KEY"),
		FreeCurrencyKey: os.Getenv("FREECURRENCY_API_KEY"),
		Port:            getEnv("PORT", "8080"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
