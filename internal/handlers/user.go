package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"service-info/internal/models"
	"service-info/internal/services"
	"strconv"
)

type UserHandler struct {
	service *services.UserService
}

func NewUserHandler(service *services.UserService) *UserHandler {
	return &UserHandler{service: service}
}

func (h *UserHandler) CreateUser(w http.ResponseWriter, r *http.Request) {
	var user models.UserData
	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Берём UserID из заголовка
	userIDStr := r.Header.Get("X-User-ID")
	if userIDStr == "" {
		http.Error(w, "X-User-ID header is required", http.StatusBadRequest)
		return
	}

	userIDInt, err := strconv.Atoi(userIDStr)
	if err != nil || userIDInt <= 0 {
		http.Error(w, "Invalid X-User-ID", http.StatusBadRequest)
		return
	}
	user.UserID = int64(userIDInt)

	// Создаём пользователя через сервис
	if err := h.service.CreateUser(user); err != nil {
		log.Printf("CreateUser failed: %v", err)
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
