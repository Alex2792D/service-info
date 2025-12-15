package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"service-info/internal/models"
	"service-info/internal/services"
)

type AdminHandler struct {
	service *services.AdminService
}

func NewAdminHandler(adminService *services.AdminService) *AdminHandler {
	return &AdminHandler{service: adminService}
}

func (h *AdminHandler) CreatePopular(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	text := strings.TrimSpace(req.Text)
	if text == "" {
		http.Error(w, "field 'text' is required", http.StatusBadRequest)
		return
	}

	task, err := parseCommandToTask(text)
	if err != nil {
		http.Error(w, "parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.service.SaveTask(r.Context(), task); err != nil {
		log.Printf("Failed to save task: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := struct {
		OK   bool              `json:"ok"`
		Type string            `json:"type"`
		Args map[string]string `json:"args"`
	}{
		OK:   true,
		Type: task.Title,
		Args: task.Args,
	}
	json.NewEncoder(w).Encode(resp)
}

func parseCommandToTask(text string) (models.Task, error) {
	parts := strings.Fields(text)
	if len(parts) == 0 {
		return models.Task{}, fmt.Errorf("empty command")
	}

	cmd := parts[0]
	args := make(models.TaskArgs)

	switch cmd {
	case "/weather":
		if len(parts) < 2 {
			return models.Task{}, fmt.Errorf("usage: /weather <city>")
		}
		args["city"] = strings.ToLower(parts[1])
		if len(parts) > 2 {
			args["lang"] = strings.ToLower(parts[2])
		}
		return models.Task{
			Title:     "weather",
			Args:      args,
			CreatedAt: time.Now(),
		}, nil

	case "/exchange":
		if len(parts) < 3 {
			return models.Task{}, fmt.Errorf("usage: /exchange <base> <target>")
		}
		args["base"] = strings.ToUpper(parts[1])
		args["target"] = strings.ToUpper(parts[2])
		return models.Task{
			Title:     "exchange",
			Args:      args,
			CreatedAt: time.Now(),
		}, nil

	default:
		return models.Task{}, fmt.Errorf("unknown command: %s", cmd)
	}
}
