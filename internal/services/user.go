package services

import (
	"encoding/json"
	"fmt"
	"log"
	"service-info/internal/messaging"
	"service-info/internal/models"
	"service-info/internal/repositories"
	"strconv" // ← добавить импорт
)

type UserService struct {
	repo     *repositories.UserRepository
	producer *messaging.Producer
}

func NewUserService(repo *repositories.UserRepository, producer *messaging.Producer) *UserService {
	return &UserService{repo: repo, producer: producer}
}

func (s *UserService) CreateUser(user models.UserData) error {
	// ✅ Добавим минимальную защиту от некорректного ID
	if user.UserID <= 0 {
		return fmt.Errorf("invalid UserID: %d", user.UserID)
	}

	// 1. Сохраняем в DB
	if err := s.repo.Save(user); err != nil {
		return err
	}
	// 2. Отправляем в Kafka
	key := []byte(strconv.FormatInt(user.UserID, 10)) // ← безопасно для любых int64
	value, _ := json.Marshal(user)

	if err := s.producer.Publish(key, value); err != nil {
		log.Printf("❌ Failed to publish user to Kafka: %v", err)
	}

	return nil
}
