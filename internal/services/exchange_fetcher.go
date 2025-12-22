package services

import (
	"strings"

	"service-info/internal/api"
	"service-info/internal/models"
)

type ExchangeFetcher struct{}

func (ExchangeFetcher) CacheKey(params ...string) string {
	base := strings.ToLower(params[0])
	target := strings.ToLower(params[1])
	return "exchange:" + base + "_" + target
}

func (ExchangeFetcher) Fetch(params ...string) (*models.ExchangeRate, error) {
	return api.FetchExchangeRate(params[0], params[1])
}
