package kafka

type KafkaBundle struct {
	WeatherProducer  *Producer
	UserProducer     *Producer
	ExchangeProducer *Producer
	PopularProducer  *Producer

	WeatherConsumer  *Consumer
	UserConsumer     *Consumer
	ExchangeConsumer *Consumer
	PopularConsumer  *Consumer
}

func InitKafka() *KafkaBundle {
	return &KafkaBundle{
		WeatherProducer:  NewProducer(getEnv("WEATHER_KAFKA_TOPIC", "weather-updates")),
		UserProducer:     NewProducer(getEnv("USER_KAFKA_TOPIC", "user-events")),
		ExchangeProducer: NewProducer(getEnv("EXCHANGE_KAFKA_TOPIC", "exchange-updates")),
		PopularProducer:  NewProducer("popular-requests"),

		WeatherConsumer:  NewConsumer(getEnv("WEATHER_KAFKA_TOPIC", "weather-updates"), "weather-redis-syncer"),
		UserConsumer:     NewConsumer(getEnv("USER_KAFKA_TOPIC", "user-events"), "user-redis-syncer"),
		ExchangeConsumer: NewConsumer(getEnv("EXCHANGE_KAFKA_TOPIC", "exchange-updates"), "exchange-redis-syncer"),
		PopularConsumer:  NewConsumer("popular-requests", "popular-syncer"),
	}
}
