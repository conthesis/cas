package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/fx"
	"github.com/minio/minio-go/v7"
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

type MinioStorage struct {
	client *minio.Client
	bucket string
}

func (s *MinioStorage) Get(ctx context.Context, pointer []byte) ([]byte, error) {
	key := base64.RawURLEncoding.EncodeToString(pointer)
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(obj)
}

func (s *MinioStorage) Store(ctx context.Context, hash []byte, data []byte) error {
	key := base64.RawURLEncoding.EncodeToString(hash)
	_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})

	// If we get a 404 response store it...
	if errResp, ok := err.(minio.ErrorResponse); ok && errResp.Code == "404" {
		_, err := s.client.PutObject(
				ctx, s.bucket, key,
				bytes.NewBuffer(data), int64(len(data)),
				minio.PutObjectOptions{
					SendContentMd5: true,
				},
		)
		return err
	} else {
		return err
	}
	return nil
}

func (s *MinioStorage) Close() error {
	return nil
}

const BucketName = "conthesis-cas"

func NewMinioStorage() (*MinioStorage, error) {
	minioEndpoint, err := getRequiredEnv("MINIO_ENDPOINT")
	if err != nil {
		return nil, err
	}
	keyId, err := getRequiredEnv("MINIO_KEY_ID")
	if err != nil {
		return nil, err
	}
	accessKey, err := getRequiredEnv("MINIO_ACCESS_KEY")
	if err != nil {
		return nil, err
	}
	useSSL := os.Getenv("MINIO_SSL") == "yes"

	mn, err := minio.New(minioEndpoint, &minio.Options{
		Secure: useSSL,
		Creds: credentials.NewStaticV4(keyId, accessKey, ""),

	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	defer cancel()

	exists, err := mn.BucketExists(ctx, BucketName)
	if err != nil {
		return nil, err
	}

	if !exists {
		err = mn.MakeBucket(ctx, BucketName, minio.MakeBucketOptions{
			Region: os.Getenv("MINIO_REGION"),
		})
		if err != nil {
			return nil, err
		}
	}

	return &MinioStorage{
		client: mn,
		bucket: BucketName,
	}, nil
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
	} else if storageDriver == "minio" {
		return NewMinioStorage()
	} else {
		return nil, ErrNoSuchStorageDriver
	}
}
