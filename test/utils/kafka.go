// test/utils/kafka.go
package testutils

import (
	"bytes"
	"os/exec"
	"testing"
)

func CreateKafkaTopic(t *testing.T, topic string) {
	t.Helper()

	cmd := exec.Command(
		"docker", "exec", "kafka",
		"kafka-topics", "--create",
		"--topic", topic,
		"--bootstrap-server", "localhost:9092",
		"--replication-factor", "1",
		"--partitions", "1",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	if err != nil {
		t.Fatalf("Не удалось создать тему %s: %v\nВывод: %s", topic, err, out.String())
	}

	t.Logf("✅ Тема %s создана", topic)
}
