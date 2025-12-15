package models

import "time"

type Weather struct {
	City         string    `json:"city"`
	Temp         float64   `json:"temp_celsius"`
	FeelsLike    float64   `json:"feels_like"`
	Humidity     int       `json:"humidity"`
	Condition    string    `json:"condition"`
	WindKPH      float64   `json:"wind_kph"`
	PressureMB   float64   `json:"pressure_mb"`
	Cloud        int       `json:"cloud_percent"`
	VisibilityKM float64   `json:"visibility_km"`
	Updated      time.Time `json:"updated_at"`
}
