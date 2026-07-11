package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/hibiken/asynq"
)

type WorkerServer struct {
	server *asynq.Server
}

func NewWorkerServer(redisURL string) (*WorkerServer, error) {
	redisOpt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url for worker: %w", err)
	}

	srv := asynq.NewServer(
		redisOpt,
		asynq.Config{
			// Concurrency limit: 100 workers
			Concurrency: 100,
			// Queue prioritization weights
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				slog.Error("Asynq Task execution failed", "task", task.Type(), "error", err)
			}),
		},
	)

	return &WorkerServer{
		server: srv,
	}, nil
}

// Start spawns the background worker server processing registered task handlers
func (w *WorkerServer) Start(mux *asynq.ServeMux) error {
	slog.Info("Starting Asynq worker server in atlas-emailagg...")
	return w.server.Start(mux)
}

// Shutdown stops the worker server gracefully
func (w *WorkerServer) Shutdown() {
	slog.Info("Shutting down Asynq worker server in atlas-emailagg...")
	w.server.Shutdown()
}
