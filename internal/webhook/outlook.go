package webhook

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"atlas-emailagg/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
)

// SetupOutlookWebhookRoutes registers push notification webhook routes for Outlook.
func SetupOutlookWebhookRoutes(r chi.Router, qClient *queue.QueueClient) {
	handler := &OutlookWebhookHandler{
		queueClient: qClient,
	}
	r.Post("/outlook", handler.HandleWebhook)
}

type OutlookWebhookHandler struct {
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

		// Resource format: Users/user-email@outlook.com/Messages/message-id
		parts := strings.Split(notification.Resource, "/")
		if len(parts) < 4 || strings.ToLower(parts[0]) != "users" {
			slog.Warn("Unexpected resource path format in Outlook notification", "resource", notification.Resource)
			continue
		}

		email := strings.ToLower(parts[1])
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
