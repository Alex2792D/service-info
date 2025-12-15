package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type Task struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Args      TaskArgs  `json:"args"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type TaskArgs map[string]string

// Value реализует driver.Valuer → позволяет сохранять в JSONB
func (t TaskArgs) Value() (driver.Value, error) {
	if t == nil {
		return nil, nil
	}
	return json.Marshal(t)
}

func (t *TaskArgs) Scan(value interface{}) error {
	if value == nil {
		*t = make(TaskArgs)
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into TaskArgs", value)
	}
	return json.Unmarshal(b, t)
}
