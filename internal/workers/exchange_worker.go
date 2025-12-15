package workers

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"service-info/internal/api"

	"github.com/redis/go-redis/v9"
)

type ExchangeWorker struct {
	messages chan []byte
	redis    *redis.Client
}

func NewExchangeWorker(messages chan []byte, redis *redis.Client) *ExchangeWorker {
	return &ExchangeWorker{messages: messages, redis: redis}
}

func (w *ExchangeWorker) Start(ctx context.Context) {
	log.Println("🚀 ExchangeWorker started")
	for {
		select {
		case msg := <-w.messages:
			var wrapper struct {
				Type string            `json:"type"`
				Args map[string]string `json:"args"`
			}
			if err := json.Unmarshal(msg, &wrapper); err != nil {
				log.Printf("❌ Failed to unmarshal exchange message: %v", err)
				continue
			}

			if wrapper.Type != "exchange" {
				continue
			}

			base := wrapper.Args["base"]
			target := wrapper.Args["target"]

			if base == "" || target == "" {
				log.Println("⚠️ ExchangeWorker: base or target empty")
				continue
			}

			// Получаем курс через API
			rate, err := api.FetchExchangeRate(base, target)
			if err != nil {
				log.Printf("❌ Failed to fetch exchange rate %s->%s: %v", base, target, err)
				continue
			}

			// Ключ Redis
			key := "exchange:" + strings.ToLower(base+"_"+target)
			data, err := json.Marshal(rate)
			if err != nil {
				log.Printf("❌ Failed to marshal exchange rate for %s: %v", key, err)
				continue
			}

			// Сохраняем в Redis
			if err := w.redis.Set(ctx, key, data, 1*time.Hour).Err(); err != nil {
				log.Printf("❌ Failed to set Redis for %s: %v", key, err)
			} else {
				log.Printf("✅ Exchange rate updated in Redis: %s = %.2f", key, rate.Rate)
			}

		case <-ctx.Done():
			log.Println("🛑 ExchangeWorker stopped")
			return
		}
	}
}
