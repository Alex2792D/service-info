package services

import (
	"context"

	"service-info/internal/models"
	"service-info/internal/repositories"
)

type AdminServiceInterface interface {
	SaveTask(ctx context.Context, task models.Task) error
}

type AdminService struct {
	repo *repositories.AdminRepository
}

func NewAdminService(repo *repositories.AdminRepository) *AdminService {
	return &AdminService{repo: repo}
}
func (s *AdminService) SaveTask(ctx context.Context, task models.Task) error {
	return s.repo.Save(ctx, task)
}
