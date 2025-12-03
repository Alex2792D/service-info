package models

import "time"

type Weather struct {
	City         string    `json:"city"`
	Temp         float64   `json:"temp_celsius"`
	FeelsLike    float64   `json:"feels_like"`
	Humidity     int       `json:"humidity"`
	Condition    string    `json:"condition"`     // ← новое
	WindKPH      float64   `json:"wind_kph"`      // ← новое
	PressureMB   float64   `json:"pressure_mb"`   // ← новое
	Cloud        int       `json:"cloud_percent"` // ← новое
	VisibilityKM float64   `json:"visibility_km"` // ← новое
	Updated      time.Time `json:"updated_at"`
}
