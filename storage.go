package main

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

// Storage is an interface common for storage engines
type Storage interface {
	// Get gets the with the passed in hash
	Get(context.Context, []byte) ([]byte, error)
	// Store stores the passed in data and returns its hash
	Store(context.Context, []byte, []byte) error
	Close()
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
	_, err := r.client.Set(ctx, string(casKey(hash)), data, 0).Result()
	return err
}

func (r *RedisStorage) Close() {
	// Nothing for now
}

// newRedisStorage creates a new redis storage
func newRedisStorage() (Storage, error) {
	redisURL := getRequiredEnv("REDIS_URL")
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return Storage(&RedisStorage{
		client: redis.NewClient(opts),
	}), nil
}

func newLevelDBStorage() (Storage, error) {
	return nil, nil
}

var ErrNoSuchStorageDriver = errors.New("No such storage driver found")

func newStorage() (Storage, error) {
	storageDriver := getRequiredEnv("STORAGE_DRIVER")
	if storageDriver == "redis" {
		return newRedisStorage()
	} else if storageDriver == "leveldb" {
		return newLevelDBStorage()
	} else {
		return nil, ErrNoSuchStorageDriver
	}
}
