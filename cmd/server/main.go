package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"atlas-emailagg/internal/config"
	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/oauth"
	"atlas-emailagg/internal/parser"
	imap "atlas-emailagg/internal/provider/imap"
	"atlas-emailagg/internal/queue"
	"atlas-emailagg/internal/webhook"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/hibiken/asynq"
)

func main() {
	// Initialize logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("Starting atlas-emailagg aggregator service...")

	// 1. Load config
	cfg := config.LoadConfig()

	// 2. Initialize database connection pool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbPool, err := db.InitPool(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("Database connection pool failed", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	// 3. Initialize Redis and Asynq client
	qClient, err := queue.InitQueue(cfg.RedisURL)
	if err != nil {
		slog.Error("Redis initialization failed", "error", err)
		os.Exit(1)
	}
	defer qClient.AsynqClient.Close()
	defer qClient.RedisClient.Close()

	// 4. Initialize and start Asynq worker server
	workerSrv, err := queue.NewWorkerServer(cfg.RedisURL)
	if err != nil {
		slog.Error("Asynq worker server initialization failed", "error", err)
		os.Exit(1)
	}

	// Initialize rule cache and invalidation listener
	ruleCache := parser.NewRuleCache(dbPool)
	parser.StartInvalidationListener(context.Background(), qClient.RedisClient, ruleCache)

	// Initialize task handlers
	taskHandler := queue.NewTaskHandler(dbPool, qClient, cfg, ruleCache)

	// Register task handlers
	mux := asynq.NewServeMux()
	mux.HandleFunc(queue.TypeEmailFetch, taskHandler.HandleEmailFetchTask)
	mux.HandleFunc(queue.TypeEmailProcess, taskHandler.HandleEmailProcessTask)
	mux.HandleFunc(queue.TypeEmailDisconnect, taskHandler.HandleEmailDisconnectTask)


	if err := workerSrv.Start(mux); err != nil {
		slog.Error("Asynq worker server failed to start", "error", err)
		os.Exit(1)
	}
	slog.Info("Asynq background worker server started successfully")

	// Start background subscription renewer daemon
	renewerCtx, renewerCancel := context.WithCancel(context.Background())
	defer renewerCancel()
	renewer := queue.NewSubscriptionRenewer(dbPool, qClient, cfg)
	renewer.Start(renewerCtx)

	// Start background IMAP IDLE pool daemon
	imapPool := imap.NewIMAPPool(dbPool, qClient, cfg)
	if cfg.EnableIMAP {
		imapPool.Start(context.Background())
		defer imapPool.Stop()
	}

	// 5. Setup HTTP Router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Health check endpoint
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "UP"}`))
	})

	// Mount OAuth and Webhook routes
	r.Route("/oauth", func(r chi.Router) {
		if cfg.EnableGmail {
			oauth.SetupGoogleRoutes(r, dbPool, qClient, cfg)
		}
		if cfg.EnableOutlook {
			oauth.SetupOutlookRoutes(r, dbPool, qClient, cfg)
		}
	})



	r.Route("/webhooks", func(r chi.Router) {
		if cfg.EnableGmail {
			webhook.SetupGoogleWebhookRoutes(r, qClient)
		}
		if cfg.EnableOutlook {
			webhook.SetupOutlookWebhookRoutes(r, qClient)
		}
	})



	// 6. Start HTTP Server
	serverAddr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         serverAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("HTTP Server listening", "addr", serverAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP Server crashed", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down atlas-emailagg gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP Server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP Server shutdown failed", "error", err)
	}

	// Shutdown Asynq Worker Server
	workerSrv.Shutdown()

	slog.Info("atlas-emailagg service stopped successfully")
}
