package webhook

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/model"
	"atlas-emailagg/internal/parser"
	"atlas-emailagg/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jhillyerd/enmime"
)

// SetupCloudflareWebhookRoutes registers webhook routes for Cloudflare Email Workers.
func SetupCloudflareWebhookRoutes(r chi.Router, dbPool *pgxpool.Pool, qClient *queue.QueueClient, ruleCache *parser.RuleCache) {
	handler := &CloudflareWebhookHandler{
		dbPool:      dbPool,
		queueClient: qClient,
		ruleCache:   ruleCache,
	}
	r.Post("/cloudflare/pre-check", handler.HandlePreCheck)
	r.Post("/cloudflare", handler.HandleWebhook)
}

type CloudflareWebhookHandler struct {
	dbPool      *pgxpool.Pool
	queueClient *queue.QueueClient
	ruleCache   *parser.RuleCache
}

type CloudflarePreCheckPayload struct {
	To      string `json:"to"`
	From    string `json:"from"`
	Subject string `json:"subject"`
}

type CloudflareWebhookPayload struct {
	MessageID string `json:"message_id"`
	To        string `json:"to"`
	From      string `json:"from"`
	Subject   string `json:"subject"`
	BodyText  string `json:"body_text"`
	BodyHTML  string `json:"body_html"`
	Date      string `json:"date"`
}

func (h *CloudflareWebhookHandler) HandlePreCheck(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read Cloudflare pre-check body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var payload CloudflarePreCheckPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		slog.Error("Failed to parse Cloudflare pre-check JSON", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	recipient := strings.ToLower(strings.TrimSpace(payload.To))
	acc, err := db.GetEmailAccountByEmailAndProvider(r.Context(), h.dbPool, recipient, "cloudflare")
	if err != nil {
		slog.Error("Database lookup for Cloudflare account failed", "error", err, "recipient", recipient)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if acc == nil || acc.Status != "ACTIVE" {
		slog.Warn("Active Cloudflare email account not found in database", "recipient", recipient)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"match": false}`))
		return
	}

	// Verify webhook token
	receivedToken := r.Header.Get("X-Atlas-Webhook-Token")
	var creds struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		slog.Error("Failed to parse Cloudflare credentials", "error", err, "recipient", recipient)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if subtle.ConstantTimeCompare([]byte(receivedToken), []byte(creds.Token)) != 1 {
		slog.Warn("Invalid X-Atlas-Webhook-Token in Cloudflare pre-check", "recipient", recipient)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Match subject rules
	rules, err := h.ruleCache.GetRules(r.Context(), acc.TenantID)
	if err != nil {
		slog.Error("Failed to fetch subject rules", "error", err, "tenant", acc.TenantID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	matched := false
	for _, rule := range rules {
		if strings.EqualFold(strings.TrimSpace(payload.Subject), strings.TrimSpace(rule.Subject)) {
			matched = true
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if matched {
		w.Write([]byte(`{"match": true}`))
	} else {
		w.Write([]byte(`{"match": false}`))
	}
}

func (h *CloudflareWebhookHandler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	recipient := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Atlas-Webhook-To")))
	subject := r.Header.Get("X-Atlas-Webhook-Subject")
	receivedToken := r.Header.Get("X-Atlas-Webhook-Token")

	acc, err := db.GetEmailAccountByEmailAndProvider(r.Context(), h.dbPool, recipient, "cloudflare")
	if err != nil {
		slog.Error("Database lookup for Cloudflare account failed", "error", err, "recipient", recipient)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if acc == nil || acc.Status != "ACTIVE" {
		slog.Warn("Active Cloudflare email account not found in database", "recipient", recipient)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Verify webhook token
	var creds struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		slog.Error("Failed to parse Cloudflare credentials", "error", err, "recipient", recipient)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if subtle.ConstantTimeCompare([]byte(receivedToken), []byte(creds.Token)) != 1 {
		slog.Warn("Invalid X-Atlas-Webhook-Token in Cloudflare webhook", "recipient", recipient)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Read binary EML bytes from the HTTP request body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read Cloudflare webhook body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse raw EML bytes using enmime
	envelope, err := enmime.ReadEnvelope(bytes.NewReader(bodyBytes))
	if err != nil {
		slog.Error("Failed to parse raw email MIME using enmime", "error", err, "recipient", recipient)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	messageID := envelope.GetHeader("Message-Id")
	if messageID == "" {
		messageID = fmt.Sprintf("cf-%d-%d", time.Now().UnixNano(), acc.ID)
	}

	// Deduplication check
	isDup, err := h.isDuplicateMessage(r.Context(), messageID)
	if err != nil {
		slog.Error("Deduplication check failed", "error", err, "message_id", messageID)
	} else if isDup {
		slog.Info("Cloudflare message already processed (duplicate), skipping", "message_id", messageID, "email", recipient)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "duplicate"}`))
		return
	}

	// Match rules again to get rule Context
	rules, err := h.ruleCache.GetRules(r.Context(), acc.TenantID)
	if err != nil {
		slog.Error("Failed to fetch subject rules in webhook", "error", err, "tenant", acc.TenantID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var matchedRule *model.SubjectRule
	for _, rule := range rules {
		if strings.EqualFold(strings.TrimSpace(subject), strings.TrimSpace(rule.Subject)) {
			matchedRule = &rule
			break
		}
	}

	if matchedRule == nil {
		slog.Info("Cloudflare email subject did not match any tenant rules, discarding", "subject", subject, "tenant", acc.TenantID)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "discarded"}`))
		return
	}

	// Parse received date from EML header, fallback to now
	var parsedDate time.Time
	if dateStr := envelope.GetHeader("Date"); dateStr != "" {
		if d, err := mail.ParseDate(dateStr); err == nil {
			parsedDate = d
		}
	}
	if parsedDate.IsZero() {
		parsedDate = time.Now()
	}

	bodyText := envelope.HTML
	if bodyText == "" {
		bodyText = envelope.Text
	}

	// Enqueue TypeEmailProcess task directly
	processPayload := map[string]interface{}{
		"tenant_id":  acc.TenantID,
		"account_id": acc.ID,
		"from":       acc.Email,
		"subject":    subject,
		"date":       parsedDate.Format(time.RFC3339),
		"body_text":  bodyText,
	}

	processBytes, err := json.Marshal(processPayload)
	if err != nil {
		slog.Error("Failed to marshal email process payload", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	processTask := asynq.NewTask(queue.TypeEmailProcess, processBytes, asynq.Queue("default"))
	_, err = h.queueClient.AsynqClient.Enqueue(processTask)
	if err != nil {
		slog.Error("Failed to enqueue Cloudflare email process task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	slog.Info("Successfully enqueued Cloudflare email process task", "email", recipient, "subject", subject)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "queued"}`))
}

func (h *CloudflareWebhookHandler) isDuplicateMessage(ctx context.Context, messageID string) (bool, error) {
	if messageID == "" {
		return false, nil
	}
	key := fmt.Sprintf("dedup:email:%s", messageID)
	res, err := h.queueClient.RedisClient.SetNX(ctx, key, "1", 24*time.Hour).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check/set message deduplication key in Redis: %w", err)
	}
	return !res, nil
}
