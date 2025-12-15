package models

type PopularRequest struct {
	Type string   `json:"type"`
	Args TaskArgs `json:"args"`
}
