package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"service-info/internal/models"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

func FetchWeather(city string) (*models.Weather, error) {
	apiKey := os.Getenv("WEATHERAPI_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("WEATHERAPI_KEY not set")
	}

	encodedCity := url.QueryEscape(city)
	apiURL := fmt.Sprintf("https://api.weatherapi.com/v1/current.json?key=%s&q=%s&lang=ru", apiKey, encodedCity)

	resp, err := httpClient.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var errResp struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(body, &errResp)
		return nil, fmt.Errorf("WeatherAPI %d: %s", resp.StatusCode, errResp.Message)
	}

	var apiResp struct {
		Location struct {
			Name string `json:"name"`
		} `json:"location"`
		Current struct {
			TempC     float64 `json:"temp_c"`
			FeelsLike float64 `json:"feelslike_c"`
			Humidity  int     `json:"humidity"`
			Condition struct {
				Text string `json:"text"`
			} `json:"condition"`
			WindKPH      float64 `json:"wind_kph"`
			PressureMB   float64 `json:"pressure_mb"`
			Cloud        int     `json:"cloud"`
			VisibilityKM float64 `json:"vis_km"`
		} `json:"current"`
	}

	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("invalid JSON format: %w", err)
	}

	return &models.Weather{
		City:         apiResp.Location.Name,
		Temp:         apiResp.Current.TempC,
		FeelsLike:    apiResp.Current.FeelsLike,
		Humidity:     apiResp.Current.Humidity,
		Condition:    apiResp.Current.Condition.Text,
		WindKPH:      apiResp.Current.WindKPH,
		PressureMB:   apiResp.Current.PressureMB,
		Cloud:        apiResp.Current.Cloud,
		VisibilityKM: apiResp.Current.VisibilityKM,
		Updated:      time.Now(),
	}, nil
}
