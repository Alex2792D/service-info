// package workers

// import (
// 	"context"
// 	"strings"

// 	"service-info/internal/api"
// 	"service-info/internal/models"
// )

// type ExchangeWorkerHandler struct{}

// func (ExchangeWorkerHandler) Type() string {
// 	return "exchange"
// }

// func (ExchangeWorkerHandler) Handle(
// 	ctx context.Context,
// 	args map[string]string,
// ) (*models.ExchangeRate, string, error) {

// 	base := strings.ToLower(args["base"])
// 	target := strings.ToLower(args["target"])

// 	rate, err := api.FetchExchangeRate(base, target)
// 	if err != nil {
// 		return nil, "", err
// 	}

// 	key := "exchange:" + base + "_" + target
// 	return rate, key, nil
// }

// func (ExchangeWorkerHandler) TTL() int {
// 	return 3600
// }

// workers/exchange_worker_handler.go
package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"service-info/internal/api"
	"service-info/internal/models"
)

type ExchangeWorkerHandler struct{}

func (ExchangeWorkerHandler) Type() string {
	return "exchange"
}

func (h ExchangeWorkerHandler) Handle(
	ctx context.Context,
	key, value []byte,
) (*models.ExchangeRate, string, error) {

	var cmd struct {
		Type string            `json:"type"`
		Args map[string]string `json:"args"`
	}
	if err := json.Unmarshal(value, &cmd); err == nil && cmd.Type == "exchange" {
		base, target := cmd.Args["base"], cmd.Args["target"]
		if base == "" || target == "" {
			return nil, "", fmt.Errorf("base and target required in command")
		}
		rate, err := api.FetchExchangeRate(base, target)
		if err != nil {
			return nil, "", err
		}
		cacheKey := "exchange:" + strings.ToLower(base) + "_" + strings.ToLower(target)
		return rate, cacheKey, nil
	}

	var rateObj models.ExchangeRate
	if err := json.Unmarshal(value, &rateObj); err != nil {
		return nil, "", fmt.Errorf("invalid exchange JSON: %w", err)
	}
	if rateObj.Base == "" || rateObj.Target == "" {
		return nil, "", fmt.Errorf("base/target empty in exchange object")
	}
	cacheKey := "exchange:" + strings.ToLower(rateObj.Base) + "_" + strings.ToLower(rateObj.Target)
	return &rateObj, cacheKey, nil
}

func (ExchangeWorkerHandler) TTL() int {
	return 3600
}
