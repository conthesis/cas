package main

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"go.uber.org/fx"
)

// Storage is an interface common for storage engines
type Storage interface {
	// Get gets the with the passed in hash
	Get(context.Context, []byte) ([]byte, error)
	// Store stores the passed in data and returns its hash
	Store(context.Context, []byte, []byte) error
	Close() error
}

// RedisStorage is storage engine storing things to Redis
type RedisStorage struct {
	client *redis.Client
}

func casKey(hash []byte) string {
	prefix := "cas:"
	return prefix + string(hash)
}

func (r *RedisStorage) Get(ctx context.Context, pointer []byte) ([]byte, error) {
	data, err := r.client.Get(ctx, string(casKey(pointer))).Result()
	if err == redis.Nil {
		return []byte(""), nil
	}
	return []byte(data), err
}

func (r *RedisStorage) Store(ctx context.Context, hash []byte, data []byte) error {
	return r.client.Set(ctx, string(casKey(hash)), data, 0).Err()
}

func (r *RedisStorage) Close() error {
	return r.client.Close()
}

// newRedisStorage creates a new redis storage
func newRedisStorage() (Storage, error) {
	redisURL, err := getRequiredEnv("REDIS_URL")
	if err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return Storage(&RedisStorage{
		client: redis.NewClient(opts),
	}), nil
}

var ErrNoSuchStorageDriver = errors.New("No such storage driver found")

func NewStorage(lc fx.Lifecycle) (Storage, error) {
	storageDriver, err := getRequiredEnv("STORAGE_DRIVER")
	if err != nil {
		return nil, err
	}
	if storageDriver == "redis" {
		res, err := newRedisStorage()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				return res.Close()
			},
		})
		return res, nil
	} else {
		return nil, ErrNoSuchStorageDriver
	}
}
