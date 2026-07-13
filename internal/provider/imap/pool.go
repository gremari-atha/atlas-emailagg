package imap

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"atlas-emailagg/internal/config"
	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/model"
	"atlas-emailagg/internal/queue"

	"github.com/emersion/go-imap/client"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

type IMAPPool struct {
	dbPool      *pgxpool.Pool
	qClient     *queue.QueueClient
	cfg         *config.Config
	cancelFuncs map[string]context.CancelFunc
	mu          sync.Mutex
}

func NewIMAPPool(dbPool *pgxpool.Pool, qClient *queue.QueueClient, cfg *config.Config) *IMAPPool {
	return &IMAPPool{
		dbPool:      dbPool,
		qClient:     qClient,
		cfg:         cfg,
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

func (p *IMAPPool) Start(ctx context.Context) {
	slog.Info("Starting IMAP IDLE pool daemon...")

	// Listen for dynamic disconnect events
	go p.listenDisconnect(ctx)

	// Perform initial sync
	p.syncActiveAccounts(ctx)

	// Periodically reconcile active IMAP accounts
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.syncActiveAccounts(ctx)
			}
		}
	}()
}

func (p *IMAPPool) syncActiveAccounts(ctx context.Context) {
	accounts, err := db.GetActiveIMAPAccounts(ctx, p.dbPool)
	if err != nil {
		slog.Error("Failed to fetch active IMAP accounts for sync", "error", err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, acc := range accounts {
		if _, exists := p.cancelFuncs[acc.ID]; exists {
			continue
		}

		accCtx, cancel := context.WithCancel(ctx)
		p.cancelFuncs[acc.ID] = cancel
		go p.startSupervisor(accCtx, acc)
	}
}

func (p *IMAPPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	slog.Info("Stopping IMAP IDLE pool daemon...")
	for id, cancel := range p.cancelFuncs {
		cancel()
		delete(p.cancelFuncs, id)
	}
}

func (p *IMAPPool) startSupervisor(ctx context.Context, acc model.EmailAccount) {
	slog.Info("Starting IMAP IDLE supervisor for account", "email", acc.Email)

	var creds model.IMAPCredentials
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		slog.Error("Failed to parse IMAP credentials for account", "email", acc.Email, "error", err)
		return
	}

	addr := fmt.Sprintf("%s:%d", creds.Host, creds.Port)

	// Circuit Breaker metrics
	var failCount int
	var windowStart time.Time

	backoff := 10 * time.Second

	for {
		select {
		case <-ctx.Done():
			slog.Info("IMAP supervisor context canceled, stopping", "email", acc.Email)
			return
		default:
		}

		err := p.connectAndIdle(ctx, acc, creds, addr)
		if err != nil {
			slog.Warn("IMAP connection dropped or failed to connect", "email", acc.Email, "error", err)

			// Update Circuit Breaker metrics
			now := time.Now()
			if windowStart.IsZero() || now.Sub(windowStart) > 10*time.Minute {
				windowStart = now
				failCount = 1
			} else {
				failCount++
			}

			if failCount >= 5 {
				slog.Error("IMAP connection suspended: 5 failures within 10 minutes", "email", acc.Email)

				db.UpdateEmailAccountStatus(ctx, p.dbPool, acc.ID, "CONNECTION_SUSPENDED", "IMAP connection suspended due to repeated failures: "+err.Error())

				p.broadcastSuspension(ctx, acc, err.Error())
				return
			}

			// Exponential backoff up to 2 minutes
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff = backoff * 2
				if backoff > 2*time.Minute {
					backoff = 2 * time.Minute
				}
			}
		} else {
			// Reset backoff and failCount on successful connection exit
			backoff = 10 * time.Second
			failCount = 0
			windowStart = time.Time{}
		}
	}
}

func (p *IMAPPool) connectAndIdle(ctx context.Context, acc model.EmailAccount, creds model.IMAPCredentials, addr string) error {
	var c *client.Client
	var err error

	if creds.Security == "ssl" {
		c, err = client.DialTLS(addr, &tls.Config{InsecureSkipVerify: true})
	} else {
		c, err = client.Dial(addr)
		if err == nil && creds.Security == "starttls" {
			err = c.StartTLS(&tls.Config{InsecureSkipVerify: true})
		}
	}

	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer c.Logout()

	if err := c.Login(creds.Username, creds.Password); err != nil {
		return fmt.Errorf("login failed: %w", err)
	}

	supported, err := c.Support("IDLE")
	if err != nil || !supported {
		return fmt.Errorf("IDLE cap check failed or not supported: %w", err)
	}

	_, err = c.Select("INBOX", false)
	if err != nil {
		return fmt.Errorf("failed to select INBOX: %w", err)
	}

	slog.Info("IMAP connection logged in and selected INBOX successfully", "email", acc.Email)

	// Set up updates listener channel
	updatesChan := make(chan client.Update, 10)
	c.Updates = updatesChan

	// Main loop: alternates between blocking IDLE and sending NOOP heartbeats
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		done := make(chan struct{})

		// Spawn keep-alive timer that exits IDLE after 20 minutes to send a NOOP heartbeat
		go func() {
			select {
			case <-time.After(20 * time.Minute):
				close(done)
			case <-ctx.Done():
				close(done)
			}
		}()

		// Spawn updates processor
		stopChan := make(chan struct{})
		go func() {
			for {
				select {
				case update := <-updatesChan:
					if update == nil {
						return
					}
					switch update.(type) {
					case *client.MailboxUpdate:
						slog.Info("IMAP Exists / message status updated, enqueuing fetch task", "email", acc.Email)
						p.enqueueFetchTask(ctx, acc)
					}
				case <-stopChan:
					return
				}
			}
		}()

		// Execute IDLE (blocks until keep-alive timer closes 'done')
		err := c.Idle(done, nil)
		close(stopChan)

		if err != nil {
			return fmt.Errorf("IDLE returned error: %w", err)
		}

		if err := c.Noop(); err != nil {
			return fmt.Errorf("NOOP heartbeat failed: %w", err)
		}

		slog.Debug("IMAP NOOP Heartbeat executed successfully", "email", acc.Email)
	}
}

func (p *IMAPPool) enqueueFetchTask(ctx context.Context, acc model.EmailAccount) {
	taskPayload, err := json.Marshal(map[string]interface{}{
		"provider": "imap",
		"email":    acc.Email,
	})
	if err != nil {
		return
	}

	task := asynq.NewTask(queue.TypeEmailFetch, taskPayload, asynq.Queue("critical"))
	_, err = p.qClient.AsynqClient.Enqueue(task)
	if err != nil {
		slog.Error("Failed to enqueue IMAP fetch task", "error", err)
	}
}

func (p *IMAPPool) broadcastSuspension(ctx context.Context, acc model.EmailAccount, errMsg string) {
	broadcastChannel := "email_events:broadcast"
	eventPayload := map[string]string{
		"tenant_id": acc.TenantID,
		"from":      acc.Email,
		"date":      time.Now().Format(time.RFC3339),
		"subject":   "System",
		"context":   "connection-status-changed",
		"data":      "CONNECTION_SUSPENDED",
	}
	eventBytes, _ := json.Marshal(eventPayload)
	p.qClient.RedisClient.Publish(ctx, broadcastChannel, string(eventBytes))
}

func (p *IMAPPool) RemoveAccount(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cancel, exists := p.cancelFuncs[id]; exists {
		slog.Info("Closing IMAP socket and stopping supervisor", "account_id", id)
		cancel()
		delete(p.cancelFuncs, id)
	}
}

func (p *IMAPPool) listenDisconnect(ctx context.Context) {
	pubsub := p.qClient.RedisClient.Subscribe(ctx, "email_connections:disconnect")
	defer pubsub.Close()

	ch := pubsub.Channel()
	slog.Info("IMAP Pool listening for disconnect events on Redis pubsub...")

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			accountID := msg.Payload
			slog.Info("Received disconnect signal on Redis pubsub", "account_id", accountID)
			p.RemoveAccount(accountID)
		}
	}
}
