package middleware

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

// PrintAllUserKeys выводит все ключи вида user:* и их значения из Redis
func PrintAllUserKeys(redisClient *redis.Client) {
	ctx := context.Background()

	// Получаем все ключи по шаблону user:*
	keys, err := redisClient.Keys(ctx, "user:*").Result()
	if err != nil {
		log.Printf("❌ Ошибка при получении ключей из Redis: %v", err)
		return
	}

	if len(keys) == 0 {
		log.Println("🔍 В Redis нет ключей с префиксом user:*")
		return
	}

	log.Printf("📋 Найдено %d ключей с префиксом user:*:", len(keys))
	for _, key := range keys {
		value, err := redisClient.Get(ctx, key).Result()
		if err != nil {
			log.Printf("  - %s → ❌ Ошибка чтения: %v", key, err)
		} else {
			log.Printf("  - %s → %s", key, value)
		}
	}
}
