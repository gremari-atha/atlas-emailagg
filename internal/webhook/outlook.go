package webhook

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SetupOutlookWebhookRoutes registers push notification webhook routes for Outlook.
func SetupOutlookWebhookRoutes(r chi.Router, dbPool *pgxpool.Pool, qClient *queue.QueueClient) {
	handler := &OutlookWebhookHandler{
		dbPool:      dbPool,
		queueClient: qClient,
	}
	r.Post("/outlook", handler.HandleWebhook)
}

type OutlookWebhookHandler struct {
	dbPool      *pgxpool.Pool
	queueClient *queue.QueueClient
}

type MicrosoftNotification struct {
	SubscriptionID string `json:"subscriptionId"`
	ClientState    string `json:"clientState"`
	Resource       string `json:"resource"`
	ResourceData   struct {
		ID        string `json:"id"`
		OdataType string `json:"@odata.type"`
	} `json:"resourceData"`
}

type MicrosoftWebhookPayload struct {
	Value []MicrosoftNotification `json:"value"`
}

func (h *OutlookWebhookHandler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	// 1. Handle Microsoft Graph Webhook Verification Handshake
	validationToken := r.URL.Query().Get("validationToken")
	if validationToken != "" {
		slog.Info("Received Microsoft Graph subscription validation token")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(validationToken))
		return
	}

	// 2. Parse push notifications payload
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read Outlook webhook body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var payload MicrosoftWebhookPayload
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		slog.Error("Failed to parse Outlook webhook JSON", "error", err, "body", string(bodyBytes))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for _, notification := range payload.Value {
		// Verify ClientState for security
		if notification.ClientState != "atlas-outlook-state-secure" {
			slog.Warn("Received Outlook notification with invalid clientState", "clientState", notification.ClientState)
			continue
		}

		var email string
		acc, err := db.GetEmailAccountBySubscriptionID(r.Context(), h.dbPool, notification.SubscriptionID)
		if err != nil {
			slog.Error("Database subscription ID lookup failed", "error", err, "subscription_id", notification.SubscriptionID)
		}
		if acc != nil {
			email = acc.Email
		} else {
			// Fallback: parse from resource path format (Users/user-email@outlook.com/Messages/message-id or Users/userID/Messages/message-id)
			parts := strings.Split(notification.Resource, "/")
			if len(parts) >= 2 && strings.ToLower(parts[0]) == "users" {
				userIdentifier := strings.ToLower(parts[1])
				if strings.Contains(userIdentifier, "@") {
					email = userIdentifier
				} else {
					// It's a Microsoft User ID (hex string). Look up by User ID in DB.
					accByUID, err := db.GetEmailAccountByMicrosoftUserID(r.Context(), h.dbPool, userIdentifier)
					if err != nil {
						slog.Error("Database Microsoft User ID lookup failed", "error", err, "user_id", userIdentifier)
					}
					if accByUID != nil {
						email = accByUID.Email
					} else {
						slog.Warn("Active Outlook email account not found by Microsoft User ID", "user_id", userIdentifier)
					}
				}
			}
		}

		if email == "" || strings.Contains(email, "/") {
			slog.Warn("Failed to resolve email address for Outlook notification", "subscription_id", notification.SubscriptionID, "resource", notification.Resource)
			continue
		}

		messageID := notification.ResourceData.ID

		slog.Info("Outlook webhook notification received", "email", email, "message_id", messageID)

		// 3. Enqueue fetch task in "critical" queue
		taskPayload, err := json.Marshal(map[string]string{
			"provider":   "outlook",
			"email":      email,
			"message_id": messageID,
		})
		if err != nil {
			slog.Error("Failed to marshal task payload", "error", err)
			continue
		}

		task := asynq.NewTask(queue.TypeEmailFetch, taskPayload, asynq.Queue("critical"))
		_, err = h.queueClient.AsynqClient.Enqueue(task)
		if err != nil {
			slog.Error("Failed to enqueue email fetch task", "error", err)
		}
	}

	// Microsoft expects 202 Accepted to prevent resending notifications
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "accepted"}`))
}
