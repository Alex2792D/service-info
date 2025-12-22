package workers

import "context"

type WorkerHandler[T any] interface {
	Type() string
	Handle(ctx context.Context, key, value []byte) (*T, string, error)
	TTL() int
}
