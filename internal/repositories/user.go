package repositories

import (
	"database/sql"
	"log"
	"service-info/internal/models"
	"time"
)

type UserRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{db: db}
}

// Save сохраняет пользователя в БД
func (r *UserRepository) Save(user models.UserData) error {
	_, err := r.db.Exec(
		`INSERT INTO users (user_id, username, first_name, last_name, created_at) 
         VALUES ($1, $2, $3, $4, $5)`,
		user.UserID, user.UserName, user.FirstName, user.LastName, time.Now(),
	)
	if err != nil {
		log.Printf("❌ Failed to save user to DB: %v", err)
		return err
	}
	log.Printf("✅ User saved to DB: %d", user.UserID)
	return nil
}
