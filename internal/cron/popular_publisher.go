package cron

import (
	"context"
	"encoding/json"
	"log"
	"time"

	messaging "service-info/internal/kafka"
	"service-info/internal/models"
	"service-info/internal/repositories"
)

type PopularPublisher struct {
	adminRepo *repositories.AdminRepository
	producer  *messaging.Producer
	topic     string
	interval  time.Duration
}

func NewPopularPublisher(
	adminRepo *repositories.AdminRepository,
	producer *messaging.Producer,
	topic string,
	interval time.Duration,
) *PopularPublisher {
	return &PopularPublisher{
		adminRepo: adminRepo,
		producer:  producer,
		topic:     topic,
		interval:  interval,
	}
}

func (p *PopularPublisher) Start(ctx context.Context) {
	log.Printf("🕗 PopularPublisher started (interval: %v, topic: %s)", p.interval, p.topic)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := p.RunOnce(ctx); err != nil {
				log.Printf("PopularPublisher iteration failed: %v", err)
			}

		case <-ctx.Done():
			log.Println("PopularPublisher stopped")
			return
		}
	}
}

func (p *PopularPublisher) RunOnce(ctx context.Context) error {
	top, err := p.adminRepo.GetTopRequests(ctx)
	if err != nil {
		return err
	}

	if len(top) == 0 {
		log.Println("No popular requests found")
		return nil
	}

	log.Printf("Publishing %d popular requests to Kafka...", len(top))

	for _, req := range top {
		value, err := json.Marshal(req)
		if err != nil {
			log.Printf("Marshal error: %v", err)
			continue
		}

		key := p.generateKey(req)
		if err := p.producer.Publish(key, value); err != nil {
			log.Printf("Kafka publish failed (key=%s): %v", string(key), err)
		} else {
			log.Printf("Published: %s", string(key))
		}
	}

	return nil
}

func (p *PopularPublisher) generateKey(req models.PopularRequest) []byte {
	switch req.Type {
	case "weather":
		city := req.Args["city"]
		return []byte("weather:" + city)
	case "exchange":
		base, target := req.Args["base"], req.Args["target"]
		return []byte("exchange:" + base + "_" + target)
	default:
		return []byte("unknown")
	}
}
