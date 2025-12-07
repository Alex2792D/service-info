package models

type ExchangeRate struct {
	Base    string  `json:"base"`
	Target  string  `json:"target"`
	Rate    float64 `json:"rate"`
	Updated string  `json:"updated_at"`
}
