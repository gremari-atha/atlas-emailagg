package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SetupResendWebhookRoutes registers push notification webhook routes for Resend.
func SetupResendWebhookRoutes(r chi.Router, dbPool *pgxpool.Pool, qClient *queue.QueueClient) {
	handler := &ResendWebhookHandler{
		dbPool:      dbPool,
		queueClient: qClient,
	}
	r.Post("/resend", handler.HandleWebhook)
}

type ResendWebhookHandler struct {
	dbPool      *pgxpool.Pool
	queueClient *queue.QueueClient
}

type ResendWebhookPayload struct {
	Type      string    `json:"type"`
	CreatedAt time.Time `json:"created_at"`
	Data      struct {
		EmailID   string   `json:"email_id"`
		MessageID string   `json:"message_id"`
		From      string   `json:"from"`
		To        []string `json:"to"`
		Subject   string   `json:"subject"`
	} `json:"data"`
}

func (h *ResendWebhookHandler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	// 1. Read raw body (important for signature verification)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read Resend webhook body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 2. Parse payload to extract type and recipient list
	var payload ResendWebhookPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		slog.Error("Failed to parse Resend webhook JSON", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if payload.Type != "email.received" {
		slog.Warn("Received non-inbound Resend webhook event type", "type", payload.Type)
		w.WriteHeader(http.StatusOK)
		return
	}

	// 3. Find the matching email account in master.email_accounts
	var email string
	var webhookSecret string
	for _, recipient := range payload.Data.To {
		recipientClean := strings.ToLower(strings.TrimSpace(recipient))
		acc, err := db.GetEmailAccountByEmailAndProvider(r.Context(), h.dbPool, recipientClean, "resend")
		if err != nil {
			slog.Error("Database lookup for Resend account failed", "error", err, "recipient", recipientClean)
			continue
		}
		if acc != nil {
			email = acc.Email
			// Parse webhook_secret from credentials JSON
			var creds struct {
				WebhookSecret string `json:"webhook_secret"`
			}
			if err := json.Unmarshal([]byte(acc.Credentials), &creds); err == nil {
				webhookSecret = creds.WebhookSecret
			}
			break
		}
	}

	if email == "" {
		slog.Warn("No matching active Resend email account found in database", "recipients", payload.Data.To)
		w.WriteHeader(http.StatusOK)
		return
	}

	// 4. Verify Svix signature
	svixId := r.Header.Get("svix-id")
	svixTimestamp := r.Header.Get("svix-timestamp")
	svixSignature := r.Header.Get("svix-signature")

	if svixId == "" || svixTimestamp == "" || svixSignature == "" {
		slog.Warn("Missing Svix signature headers in Resend webhook", "email", email)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if err := verifySvixSignature(svixId, svixTimestamp, svixSignature, string(bodyBytes), webhookSecret); err != nil {
		slog.Warn("Invalid Svix signature in Resend webhook", "error", err, "email", email)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	slog.Info("Resend webhook signature verified", "email", email, "email_id", payload.Data.EmailID)

	// 5. Enqueue Fetch Task to critical queue
	taskPayload, err := json.Marshal(map[string]string{
		"provider":   "resend",
		"email":      email,
		"message_id": payload.Data.EmailID, // Resend email_id maps to message_id for deduplication
	})
	if err != nil {
		slog.Error("Failed to marshal Resend fetch task payload", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	task := asynq.NewTask(queue.TypeEmailFetch, taskPayload, asynq.Queue("critical"))
	_, err = h.queueClient.AsynqClient.Enqueue(task)
	if err != nil {
		slog.Error("Failed to enqueue Resend email fetch task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "accepted"}`))
}

func verifySvixSignature(svixId, svixTimestamp, svixSignature, rawBody, secret string) error {
	// 1. Replay prevention: verify timestamp is within 5 minutes
	timestampInt, err := time.Parse(time.RFC3339, svixTimestamp)
	if err != nil {
		// Try parsing as unix timestamp string
		var unixSeconds int64
		if _, err := fmt.Sscanf(svixTimestamp, "%d", &unixSeconds); err == nil {
			timestampInt = time.Unix(unixSeconds, 0)
		} else {
			return fmt.Errorf("invalid svix-timestamp format: %w", err)
		}
	}
	if time.Since(timestampInt) > 5*time.Minute || time.Until(timestampInt) > 5*time.Minute {
		return fmt.Errorf("message timestamp expired or in the future")
	}

	// 2. Decode the webhook secret
	secretBytes, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	if err != nil {
		return fmt.Errorf("invalid webhook secret encoding: %w", err)
	}

	// 3. Construct the signed content
	signedContent := fmt.Sprintf("%s.%s.%s", svixId, svixTimestamp, rawBody)

	// 4. Compute HMAC-SHA256
	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte(signedContent))
	expectedSignatureBytes := mac.Sum(nil)
	expectedSignature := base64.StdEncoding.EncodeToString(expectedSignatureBytes)

	// 5. Verify against signature headers
	signatures := strings.Split(svixSignature, " ")
	for _, sig := range signatures {
		if strings.HasPrefix(sig, "v1,") {
			candidateSig := sig[3:]
			if hmac.Equal([]byte(candidateSig), []byte(expectedSignature)) {
				return nil
			}
		}
	}

	return fmt.Errorf("signature mismatch")
}
