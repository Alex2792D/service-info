package services

import (
	"context"
	"encoding/json"
	"log"
	"service-info/internal/kafka"

	"github.com/redis/go-redis/v9"
)

type CacheService[T any] struct {
	redis    *redis.Client
	producer *kafka.Producer
	fetcher  Fetcher[T]
}

func NewCacheService[T any](
	redis *redis.Client,
	producer *kafka.Producer,
	fetcher Fetcher[T],
) *CacheService[T] {
	return &CacheService[T]{
		redis:    redis,
		producer: producer,
		fetcher:  fetcher,
	}
}

func (s *CacheService[T]) Get(params ...string) (*T, error) {
	ctx := context.Background()
	key := s.fetcher.CacheKey(params...)

	if data, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var result T
		if json.Unmarshal(data, &result) == nil {
			log.Printf("Cache HIT: %s", key)
			return &result, nil
		}
	}

	result, err := s.fetcher.Fetch(params...)
	if err != nil {
		return nil, err
	}

	if s.producer != nil {
		s.producer.PublishObjectAsync([]byte(key), result)
	}

	return result, nil
}
