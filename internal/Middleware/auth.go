// internal/middleware/auth.go
package middleware

import (
	"context"
	"log"
	"net/http"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type contextKey string

const UserIDKey contextKey = "user_id"

// AuthRequired проверяет, что user:<id> существует в Redis
func AuthRequired(redisClient *redis.Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userIDStr := r.Header.Get("X-User-ID")
			if userIDStr == "" {
				http.Error(w, "X-User-ID header required", http.StatusUnauthorized)
				return
			}

			userID, err := strconv.ParseInt(userIDStr, 10, 64)
			if err != nil {
				http.Error(w, "Invalid X-User-ID", http.StatusBadRequest)
				return
			}

			// Проверяем наличие ключа в Redis (например: "user:123456")
			ctx := r.Context()
			exists, err := redisClient.Exists(ctx, "user:"+userIDStr).Result()
			if err != nil {
				log.Printf("❌ Redis EXISTS error for user:%s: %v", userIDStr, err)
				http.Error(w, "Internal error", http.StatusInternalServerError)
				return
			}

			if exists == 0 {
				http.Error(w, "User not registered. Please use /auth in Telegram bot.", http.StatusUnauthorized)
				return
			}

			// Кладём user_id в контекст — можно использовать в handler'ах
			ctx = context.WithValue(ctx, UserIDKey, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetUserIDFromContext — для handler'ов
func GetUserIDFromContext(r *http.Request) (int64, bool) {
	userID, ok := r.Context().Value(UserIDKey).(int64)
	return userID, ok
}
