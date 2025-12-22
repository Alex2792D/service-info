// package bootstrap

// import (
// 	"database/sql"
// 	"service-info/internal/handlers"
// 	"service-info/internal/kafka"
// 	"service-info/internal/repositories"
// 	"service-info/internal/services"

// 	"github.com/redis/go-redis/v9"
// )

// type HandlersBundle struct {
// 	UserHandler     *handlers.UserHandler
// 	WeatherHandler  *handlers.WeatherHandler
// 	ExchangeHandler *handlers.ExchangeHandler
// 	AdminHandler    *handlers.AdminHandler
// }

// type BootstrapBundle struct {
// 	Handlers     *HandlersBundle
// 	Repositories struct {
// 		UserRepo  *repositories.UserRepository
// 		AdminRepo *repositories.AdminRepository
// 	}
// }

// func InitBootstrap(db *sql.DB, redisClient *redis.Client, kafkaBundle *kafka.KafkaBundle) *BootstrapBundle {
// 	userRepo := repositories.NewUserRepository(db)
// 	adminRepo := repositories.NewAdminRepository(db)

// 	handlers := &HandlersBundle{
// 		UserHandler:     handlers.NewUserHandler(services.NewUserService(userRepo, kafkaBundle.UserProducer)),
// 		WeatherHandler:  handlers.NewWeatherHandler(services.NewWeatherService(redisClient, kafkaBundle.WeatherProducer)),
// 		ExchangeHandler: handlers.NewExchangeHandler(services.NewExchangeService(redisClient, kafkaBundle.ExchangeProducer)),
// 		AdminHandler:    handlers.NewAdminHandler(services.NewAdminService(adminRepo)),
// 	}

// 	return &BootstrapBundle{
// 		Handlers: handlers,
// 		Repositories: struct {
// 			UserRepo  *repositories.UserRepository
// 			AdminRepo *repositories.AdminRepository
// 		}{
// 			UserRepo:  userRepo,
// 			AdminRepo: adminRepo,
// 		},
// 	}
// }

package bootstrap

import (
	"database/sql"

	"service-info/internal/handlers"
	"service-info/internal/kafka"
	"service-info/internal/repositories"
	"service-info/internal/services"

	"github.com/redis/go-redis/v9"
)

type HandlersBundle struct {
	UserHandler     *handlers.UserHandler
	WeatherHandler  *handlers.WeatherHandler
	ExchangeHandler *handlers.ExchangeHandler
	AdminHandler    *handlers.AdminHandler
}

type BootstrapBundle struct {
	Handlers     *HandlersBundle
	Repositories struct {
		UserRepo  *repositories.UserRepository
		AdminRepo *repositories.AdminRepository
	}
}

func InitBootstrap(
	db *sql.DB,
	redisClient *redis.Client,
	kafkaBundle *kafka.KafkaBundle,
) *BootstrapBundle {

	// =====================
	// Repositories
	// =====================
	userRepo := repositories.NewUserRepository(db)
	adminRepo := repositories.NewAdminRepository(db)

	// =====================
	// Services (polymorphic)
	// =====================

	weatherService := services.NewCacheService(
		redisClient,
		kafkaBundle.WeatherProducer,
		services.WeatherFetcher{},
	)

	exchangeService := services.NewCacheService(
		redisClient,
		kafkaBundle.ExchangeProducer,
		services.ExchangeFetcher{},
	)

	userService := services.NewUserService(
		userRepo,
		kafkaBundle.UserProducer,
	)

	adminService := services.NewAdminService(adminRepo)

	// =====================
	// Handlers
	// =====================
	handlersBundle := &HandlersBundle{
		UserHandler: handlers.NewUserHandler(userService),

		WeatherHandler: handlers.NewWeatherHandler(
			weatherService,
		),

		ExchangeHandler: handlers.NewExchangeHandler(
			exchangeService,
		),

		AdminHandler: handlers.NewAdminHandler(adminService),
	}

	return &BootstrapBundle{
		Handlers: handlersBundle,
		Repositories: struct {
			UserRepo  *repositories.UserRepository
			AdminRepo *repositories.AdminRepository
		}{
			UserRepo:  userRepo,
			AdminRepo: adminRepo,
		},
	}
}
