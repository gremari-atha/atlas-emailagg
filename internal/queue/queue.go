package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

type QueueClient struct {
	AsynqClient *asynq.Client
	RedisClient *redis.Client
}

// InitQueue sets up connections to Redis for pub/sub and task queueing.
func InitQueue(redisURL string) (*QueueClient, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}

	rClient := redis.NewClient(opt)
	if err := rClient.Ping(context.Background()).Err(); err != nil {

		rClient.Close()
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	// Initialize Asynq client using the same options
	asynqOpt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		rClient.Close()
		return nil, fmt.Errorf("failed to parse asynq redis uri: %w", err)
	}

	aClient := asynq.NewClient(asynqOpt)

	slog.Info("Successfully connected to Redis & Asynq client")

	return &QueueClient{
		AsynqClient: aClient,
		RedisClient: rClient,
	}, nil
}
