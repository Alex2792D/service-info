// package workers

// import (
// 	"context"
// 	"encoding/json"
// 	"log"
// 	"time"

// 	"github.com/redis/go-redis/v9"
// )

// type GenericWorker[T any] struct {
// 	messages chan []byte
// 	redis    *redis.Client
// 	handler  WorkerHandler[T]
// }
// type Worker interface {
// 	Start(ctx context.Context)
// }

// func NewGenericWorker[T any](
// 	messages chan []byte,
// 	redis *redis.Client,
// 	handler WorkerHandler[T],
// ) *GenericWorker[T] {
// 	return &GenericWorker[T]{
// 		messages: messages,
// 		redis:    redis,
// 		handler:  handler,
// 	}
// }

// func (w *GenericWorker[T]) Start(ctx context.Context) {
// 	log.Printf("🚀 %sWorker started", w.handler.Type())

// 	for {
// 		select {
// 		case msg := <-w.messages:
// 			var wrapper struct {
// 				Type string            `json:"type"`
// 				Args map[string]string `json:"args"`
// 			}

// 			if err := json.Unmarshal(msg, &wrapper); err != nil {
// 				log.Printf("Invalid message: %v", err)
// 				continue
// 			}

// 			if wrapper.Type != w.handler.Type() {
// 				continue
// 			}

// 			result, key, err := w.handler.Handle(ctx, wrapper.Args)
// 			if err != nil {
// 				log.Printf("%sWorker error: %v", w.handler.Type(), err)
// 				continue
// 			}

// 			data, err := json.Marshal(result)
// 			if err != nil {
// 				log.Printf("Marshal error: %v", err)
// 				continue
// 			}

// 			ttl := time.Duration(w.handler.TTL()) * time.Second
// 			if err := w.redis.Set(ctx, key, data, ttl).Err(); err != nil {
// 				log.Printf("Redis SET error %s: %v", key, err)
// 			} else {
// 				log.Printf("%s cached in Redis: %s", w.handler.Type(), key)
// 			}

// 		case <-ctx.Done():
// 			log.Printf("%sWorker stopped", w.handler.Type())
// 			return
// 		}
// 	}
// }

// workers/generic_worker.go
package workers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type GenericWorker[T any] struct {
	messages chan []byte
	redis    *redis.Client
	handler  WorkerHandler[T]
}

type Worker interface {
	Start(ctx context.Context)
}

func NewGenericWorker[T any](
	messages chan []byte,
	redis *redis.Client,
	handler WorkerHandler[T],
) *GenericWorker[T] {
	return &GenericWorker[T]{
		messages: messages,
		redis:    redis,
		handler:  handler,
	}
}

func (w *GenericWorker[T]) Start(ctx context.Context) {
	log.Printf("🚀 %sWorker started", w.handler.Type())

	for {
		select {
		case value := <-w.messages:
			result, cacheKey, err := w.handler.Handle(ctx, nil, value)
			if err != nil {
				log.Printf("%sWorker error: %v", w.handler.Type(), err)
				continue
			}

			data, err := json.Marshal(result)
			if err != nil {
				log.Printf("Marshal error: %v", err)
				continue
			}

			w.writeToRedis(ctx, cacheKey, data)

		case <-ctx.Done():
			log.Printf("%sWorker stopped", w.handler.Type())
			return
		}
	}
}

func (w *GenericWorker[T]) writeToRedis(ctx context.Context, key string, data []byte) {
	ttl := time.Duration(w.handler.TTL()) * time.Second
	if err := w.redis.Set(ctx, key, data, ttl).Err(); err != nil {
		log.Printf("Redis SET error %s: %v", key, err)
	} else {
		log.Printf("%s cached in Redis: %s", w.handler.Type(), key)
	}
}
