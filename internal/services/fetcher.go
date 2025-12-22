package services

type Fetcher[T any] interface {
	CacheKey(params ...string) string
	Fetch(params ...string) (*T, error)
}
