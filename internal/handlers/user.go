package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"service-info/internal/models"
	"service-info/internal/services"
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

	// ✅ Валидация
	if user.UserID <= 0 {
		http.Error(w, "UserID is required and must be > 0", http.StatusBadRequest)
		return
	}

	if err := h.service.CreateUser(user); err != nil {
		log.Printf("❌ CreateUser failed: %v", err)
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
