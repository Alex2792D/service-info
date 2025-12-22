
include ../../.env


.PHONY: up down wait-ready test

up:
	docker-compose -f docker-compose.test.yml up -d --remove-orphans

wait-ready:
	@echo "⏳ Ждём готовности всех сервисов..."
	@docker-compose -f docker-compose.test.yml up --wait

down:
	docker-compose -f docker-compose.test.yml down -v

test: down up wait-ready
	WEATHERAPI_KEY=$(WEATHERAPI_KEY) \
	KAFKA_BROKERS=$(KAFKA_BROKERS) \
	WEATHER_KAFKA_TOPIC=$(WEATHER_KAFKA_TOPIC) \
	FREECURRENCY_API_KEY=$(FREECURRENCY_API_KEY) \
	USER_KAFKA_TOPIC=$(USER_KAFKA_TOPIC) \
	go test -v ./... -count=1 -timeout=60s
	$(MAKE) down