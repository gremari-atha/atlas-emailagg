package webhook

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"atlas-emailagg/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/hibiken/asynq"
)

// SetupGoogleWebhookRoutes registers push notification webhook routes for Gmail.
func SetupGoogleWebhookRoutes(r chi.Router, qClient *queue.QueueClient) {
	handler := &GoogleWebhookHandler{
		queueClient: qClient,
	}
	r.Post("/google", handler.HandleWebhook)
}

type GoogleWebhookHandler struct {
	queueClient *queue.QueueClient
}

type GooglePubSubEnvelope struct {
	Message struct {
		Data      string `json:"data"`
		MessageID string `json:"messageId"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

type GooglePubSubData struct {
	EmailAddress string `json:"emailAddress"`
	HistoryID    uint64 `json:"historyId"`
}

func (h *GoogleWebhookHandler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("Failed to read Google webhook body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var envelope GooglePubSubEnvelope
	if err := json.Unmarshal(bodyBytes, &envelope); err != nil {
		slog.Error("Failed to parse Google webhook envelope", "error", err, "body", string(bodyBytes))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if envelope.Message.Data == "" {
		slog.Warn("Received empty Google Pub/Sub push notification")
		w.WriteHeader(http.StatusOK)
		return
	}

	// Decode base64 data
	decodedBytes, err := base64.StdEncoding.DecodeString(envelope.Message.Data)
	if err != nil {
		slog.Error("Failed to decode Google Pub/Sub base64 data", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var pubsubData GooglePubSubData
	if err := json.Unmarshal(decodedBytes, &pubsubData); err != nil {
		slog.Error("Failed to parse Google Pub/Sub inner JSON", "error", err, "decoded", string(decodedBytes))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	email := strings.ToLower(strings.TrimSpace(pubsubData.EmailAddress))
	if email == "" {
		slog.Warn("Google webhook inner payload lacks emailAddress")
		w.WriteHeader(http.StatusOK)
		return
	}

	slog.Info("Google Pub/Sub notification received", "email", email, "history_id", pubsubData.HistoryID)

	// Enqueue TypeEmailFetch task to critical queue
	taskPayload, err := json.Marshal(map[string]interface{}{
		"provider":   "gmail",
		"email":      email,
		"history_id": pubsubData.HistoryID,
	})
	if err != nil {
		slog.Error("Failed to marshal Gmail fetch task payload", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	task := asynq.NewTask(queue.TypeEmailFetch, taskPayload, asynq.Queue("critical"))
	_, err = h.queueClient.AsynqClient.Enqueue(task)
	if err != nil {
		slog.Error("Failed to enqueue Gmail fetch task", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "accepted"}`))
}
