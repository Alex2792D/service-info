package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"service-info/internal/models"
)

var httpClientExchange = &http.Client{Timeout: 10 * time.Second}

func FetchExchangeRate(base, target string) (*models.ExchangeRate, error) {
	apiKey := os.Getenv("FREECURRENCY_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("FREECURRENCY_API_KEY not set")
	}

	apiURL := fmt.Sprintf(
		"https://api.freecurrencyapi.com/v1/latest?apikey=%s&base_currency=%s&currencies=%s",
		url.QueryEscape(apiKey),
		url.QueryEscape(base),
		url.QueryEscape(target),
	)

	resp, err := httpClientExchange.Get(apiURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Data map[string]float64 `json:"data"`
		Meta struct {
			LastUpdated string `json:"last_updated_at"`
		} `json:"meta"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("JSON parse error: %w", err)
	}

	rate, ok := result.Data[strings.ToUpper(target)]
	if !ok {
		return nil, fmt.Errorf("currency %s not found", target)
	}

	return &models.ExchangeRate{
		Base:    base,
		Target:  target,
		Rate:    rate,
		Updated: result.Meta.LastUpdated,
	}, nil
}
