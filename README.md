# Service Info

Проект на Node.js с использованием PostgreSQL, Redis, Kafka и Liquibase.

---

## Требования

- Docker
- Docker Compose
- Node.js (для сборки Docker-образа приложения)
- `.env` файл с переменными окружения

---

## Настройка проекта

## 🚀 Быстрый старт (одной командой)

```bash
git clone https://github.com/Alex2792D/service-info.git
cd service-info

cp .env.example .env

<details>
<summary>
WEATHERAPI_KEY=622256b9dda24a82b29124055252801
FREECURRENCY_API_KEY=fca_live_ufym9fxsdW5Qz8zVSChBpGY7j6XZtAQnKc0mupAE
</summary>

docker-compose up --build
docker compose logs -f app
docker compose down -v
```

## 📊 Архитектура

![C4 Container Diagram](diagram.drawio)

> Диаграмма описывает взаимодействие между:
>
> - Frontend (User/Admin),
> - Go-сервисом (`Service-Info`),
> - PostgreSQL, Redis, Kafka,
> - Внешними API (WeatherAPI, FreeCurrencyAPI).
