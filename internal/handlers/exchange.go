package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"service-info/internal/services"
)

type ExchangeHandler struct {
	service *services.ExchangeService
}

func NewExchangeHandler(service *services.ExchangeService) *ExchangeHandler {
	return &ExchangeHandler{service: service}
}

func (h *ExchangeHandler) GetRate(w http.ResponseWriter, r *http.Request) {
	base := r.URL.Query().Get("base")
	target := r.URL.Query().Get("to")
	if base == "" || target == "" {
		http.Error(w, "❌ Требуются параметры: ?base=USD&to=RUB", http.StatusBadRequest)
		return
	}

	rate, err := h.service.GetRate(base, target)
	if err != nil {
		log.Printf("❌ Exchange error (%s→%s): %v", base, target, err)
		http.Error(w, "Не удалось получить курс", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rate)
}
