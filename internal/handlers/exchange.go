package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"service-info/internal/models"
	"service-info/internal/services"
)

type ExchangeHandler struct {
	service *services.CacheService[models.ExchangeRate]
}

func NewExchangeHandler(service *services.CacheService[models.ExchangeRate]) *ExchangeHandler {
	return &ExchangeHandler{service: service}
}

func (h *ExchangeHandler) GetRate(w http.ResponseWriter, r *http.Request) {
	base := strings.TrimSpace(r.URL.Query().Get("base"))
	target := strings.TrimSpace(r.URL.Query().Get("target"))

	if base == "" || target == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Параметры 'base' и 'target' обязательны"})
		return
	}

	rate, err := h.service.Get(base, target)
	if err != nil {
		log.Printf("Ошибка для %s -> %s: %v", base, target, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Не удалось получить курс валют"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rate)
}
