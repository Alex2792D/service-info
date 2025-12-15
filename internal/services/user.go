package services

import (
	"fmt"
	"service-info/internal/kafka"
	"service-info/internal/models"
	"service-info/internal/repositories"
	"strconv"
)

type UserServiceInterface interface {
	CreateUser(user models.UserData) error
}

type UserService struct {
	repo     *repositories.UserRepository
	producer *kafka.Producer
}

func NewUserService(repo *repositories.UserRepository, producer *kafka.Producer) *UserService {
	return &UserService{repo: repo, producer: producer}
}

func (s *UserService) CreateUser(user models.UserData) error {
	if user.UserID <= 0 {
		return fmt.Errorf("invalid UserID: %d", user.UserID)
	}

	if err := s.repo.Save(user); err != nil {
		return err
	}

	if s.producer != nil {
		kafkaKey := []byte(strconv.FormatInt(user.UserID, 10))
		s.producer.PublishObjectAsync(kafkaKey, user)
	}
	return nil
}
