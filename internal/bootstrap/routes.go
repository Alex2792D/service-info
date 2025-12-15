package bootstrap

import (
	"net/http"
	"service-info/internal/handlers"
	"service-info/internal/middleware"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
)

func InitRoutes(
	userHandler *handlers.UserHandler,
	adminHandler *handlers.AdminHandler,
	weatherHandler *handlers.WeatherHandler,
	exchangeHandler *handlers.ExchangeHandler,
	redisClient *redis.Client,
) chi.Router {

	r := chi.NewRouter()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("OK"))
	})

	r.Post("/user", userHandler.CreateUser)
	r.Post("/admin", adminHandler.CreatePopular)

	r.Group(func(r chi.Router) {
		r.Use(middleware.AuthRequired(redisClient))
		r.Get("/weather", weatherHandler.GetWeather)
		r.Get("/exchange", exchangeHandler.GetRate)
	})

	return r
}
