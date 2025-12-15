package workers

import (
	"encoding/json"
	"log"

	"service-info/internal/kafka"
)

func StartWorkerMultiplexer(consumer *kafka.Consumer, weatherCh, exchangeCh chan []byte) {
	consumer.Start(func(key, value []byte) {
		var wrapper struct {
			Type string            `json:"type"`
			Args map[string]string `json:"args"`
		}
		if err := json.Unmarshal(value, &wrapper); err != nil {
			log.Printf("Invalid message in multiplexer: %v", err)
			return
		}

		switch wrapper.Type {
		case "weather":
			weatherCh <- value
		case "exchange":
			exchangeCh <- value
		default:
			log.Printf("Unknown message type: %s", wrapper.Type)
		}
	})
}
