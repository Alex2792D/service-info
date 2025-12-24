package workers

import (
	"log"

	"service-info/internal/kafka"
)

func StartSoloMultiplexer(consumer *kafka.Consumer, outCh chan []byte) {
	if consumer == nil || outCh == nil {
		return
	}
	consumer.Start(func(key, value []byte) {
		select {
		case outCh <- value:
		default:
			log.Printf("Channel full, dropping message (key=%s)", string(key))
		}
	})
}
