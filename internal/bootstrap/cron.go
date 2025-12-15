package bootstrap

import (
	"context"
	"time"

	"service-info/internal/cron"
	"service-info/internal/kafka"
	"service-info/internal/repositories"
)

func StartCronJobs(ctx context.Context, adminRepo *repositories.AdminRepository, kafkaBundle *kafka.KafkaBundle, popularTopic string) {
	popularPublisher := cron.NewPopularPublisher(adminRepo, kafkaBundle.UserProducer, popularTopic, 5*time.Minute)
	go popularPublisher.Start(ctx)
}
