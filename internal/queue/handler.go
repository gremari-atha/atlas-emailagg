package queue

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"atlas-emailagg/internal/config"
	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/model"
	"atlas-emailagg/internal/parser"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TaskHandler struct {
	dbPool      *pgxpool.Pool
	queueClient *QueueClient
	cfg         *config.Config
	ruleCache   *parser.RuleCache
}

func NewTaskHandler(dbPool *pgxpool.Pool, qClient *QueueClient, cfg *config.Config, rCache *parser.RuleCache) *TaskHandler {
	return &TaskHandler{
		dbPool:      dbPool,
		queueClient: qClient,
		cfg:         cfg,
		ruleCache:   rCache,
	}
}

type FetchTaskPayload struct {
	Provider  string `json:"provider"`
	Email     string `json:"email"`
	MessageID string `json:"message_id,omitempty"`
	HistoryID uint64 `json:"history_id,omitempty"`
}

type MSGraphMessageHeader struct {
	Subject          string    `json:"subject"`
	ReceivedDateTime time.Time `json:"receivedDateTime"`
	From             struct {
		EmailAddress struct {
			Address string `json:"address"`
			Name    string `json:"name"`
		} `json:"emailAddress"`
	} `json:"from"`
}

type MSGraphMessageBody struct {
	Body struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	} `json:"body"`
}

type OutlookRefreshResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

// Google specific API structures
type GmailMessageMetadata struct {
	ID           string `json:"id"`
	HistoryID    string `json:"historyId"`
	InternalDate string `json:"internalDate"`
	Payload      struct {
		Headers []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"headers"`
	} `json:"payload"`
}

type GmailMessageFull struct {
	ID        string    `json:"id"`
	HistoryID string    `json:"historyId"`
	Payload   GmailPart `json:"payload"`
}

type GmailPart struct {
	MimeType string `json:"mimeType"`
	Body     struct {
		AttachmentID string `json:"attachmentId"`
		Data         string `json:"data"`
	} `json:"body"`
	Parts []GmailPart `json:"parts"`
}

type GmailHistoryResponse struct {
	History []struct {
		ID            string `json:"id"`
		MessagesAdded []struct {
			Message struct {
				ID        string `json:"id"`
				HistoryID string `json:"historyId"`
			} `json:"message"`
		} `json:"messagesAdded"`
	} `json:"history"`
	NextPageToken string `json:"nextPageToken"`
}

type GmailListResponse struct {
	Messages []struct {
		ID       string `json:"id"`
		ThreadID string `json:"threadId"`
	} `json:"messages"`
}

type ProcessTaskPayload struct {
	TenantID  string `json:"tenant_id"`
	AccountID string `json:"account_id"`
	From      string `json:"from"`
	Subject   string `json:"subject"`
	Date      string `json:"date"`
	BodyText  string `json:"body_text"`
}

func (h *TaskHandler) HandleEmailFetchTask(ctx context.Context, t *asynq.Task) error {
	var payload FetchTaskPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		slog.Error("Failed to unmarshal fetch task payload", "error", err)
		return fmt.Errorf("invalid payload: %w", err)
	}

	slog.Info("Running email fetch task dispatcher", "provider", payload.Provider, "email", payload.Email)

	switch strings.ToLower(payload.Provider) {
	case "outlook":
		return h.HandleOutlookFetch(ctx, t, payload)
	case "gmail":
		return h.HandleGmailFetch(ctx, t, payload)
	case "imap":
		return h.HandleIMAPFetch(ctx, t, payload)
	case "resend":
		return h.HandleResendFetch(ctx, t, payload)
	default:
		slog.Warn("Unsupported provider in fetch task", "provider", payload.Provider)
		return nil
	}
}

func (h *TaskHandler) rescheduleFetchTask(ctx context.Context, t *asynq.Task, delaySeconds int) {
	task := asynq.NewTask(t.Type(), t.Payload(), asynq.Queue("critical"))
	_, err := h.queueClient.AsynqClient.Enqueue(task, asynq.ProcessIn(time.Duration(delaySeconds)*time.Second))
	if err != nil {
		slog.Error("Failed to reschedule rate-limited task", "error", err)
	}
}

func (h *TaskHandler) HandleOutlookFetch(ctx context.Context, t *asynq.Task, payload FetchTaskPayload) error {
	acc, err := db.GetEmailAccountByEmailAndProvider(ctx, h.dbPool, payload.Email, "outlook")
	if err != nil {
		return fmt.Errorf("db fetch email account failed: %w", err)
	}
	if acc == nil {
		slog.Warn("Active Outlook email account not found in database", "email", payload.Email)
		return nil
	}

	// Deduplication check
	isDup, err := h.isDuplicateMessage(ctx, payload.MessageID)
	if err != nil {
		slog.Error("Deduplication check failed", "error", err, "message_id", payload.MessageID)
	} else if isDup {
		slog.Info("Outlook message already processed (duplicate), skipping", "message_id", payload.MessageID, "email", payload.Email)
		return nil
	}

	var creds model.OutlookCredentials
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		return fmt.Errorf("failed to parse credentials JSON: %w", err)
	}

	accessToken := creds.AccessToken
	if time.Now().Add(5 * time.Minute).After(creds.Expiry) {
		slog.Info("Outlook access token expired or expiring soon, refreshing", "email", acc.Email)
		refreshedCreds, err := h.refreshOutlookToken(ctx, creds.RefreshToken)
		if err != nil {
			slog.Error("Failed to refresh Outlook access token", "error", err, "email", acc.Email)
			db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", "OAuth token refresh failed: "+err.Error())
			h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
			return err
		}

		credsBytes, _ := json.Marshal(refreshedCreds)
		if err := db.UpdateEmailAccountCredentialsAndStatus(ctx, h.dbPool, acc.ID, string(credsBytes), "ACTIVE"); err != nil {
			slog.Error("Failed to save refreshed credentials", "error", err)
		}
		accessToken = refreshedCreds.AccessToken
	}

	// Fetch message headers: subject, from, and receivedDateTime
	reqURL := fmt.Sprintf("https://graph.microsoft.com/v1.0/users/%s/messages/%s?$select=subject,from,receivedDateTime", url.PathEscape(payload.Email), url.PathEscape(payload.MessageID))
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("MS Graph header request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		slog.Error("Outlook Graph API header request unauthorized/forbidden, token revoked", "email", acc.Email, "status", resp.Status, "body", string(bodyBytes))
		db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", fmt.Sprintf("Graph API header request failed: status %s, body %s", resp.Status, string(bodyBytes)))
		h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
		return nil
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := 10 // default
		if hValue := resp.Header.Get("Retry-After"); hValue != "" {
			if seconds, err := strconv.Atoi(hValue); err == nil {
				retryAfter = seconds
			}
		}
		slog.Warn("Rate limited (429) by MS Graph header API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
		h.rescheduleFetchTask(ctx, t, retryAfter)
		return nil
	}

	if resp.StatusCode == http.StatusNotFound {
		slog.Warn("Message not found in Microsoft Graph", "message_id", payload.MessageID)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to fetch email headers, status: %s, body: %s", resp.Status, string(bodyBytes))
	}

	var header MSGraphMessageHeader
	if err := json.NewDecoder(resp.Body).Decode(&header); err != nil {
		return err
	}

	// Compute time window filter start: max(LastSyncAt, now - 10 minutes)
	now := time.Now()
	tenMinutesAgo := now.Add(-10 * time.Minute)
	filterStart := tenMinutesAgo
	if acc.LastSyncAt != nil && acc.LastSyncAt.After(tenMinutesAgo) {
		filterStart = *acc.LastSyncAt
	}

	if header.ReceivedDateTime.Before(filterStart) {
		slog.Info("Outlook message is older than filter start time, skipping", "message_id", payload.MessageID, "received", header.ReceivedDateTime, "filter_start", filterStart, "email", acc.Email)
		return nil
	}

	rules, err := h.ruleCache.GetRules(ctx, acc.TenantID)
	if err != nil {
		return fmt.Errorf("failed to fetch subject rules: %w", err)
	}

	var matchedRule *model.SubjectRule
	for _, rule := range rules {
		if strings.EqualFold(strings.TrimSpace(header.Subject), strings.TrimSpace(rule.Subject)) {
			matchedRule = &rule
			break
		}
	}

	if matchedRule == nil {
		slog.Info("Outlook email subject did not match any tenant rules, discarding", "subject", header.Subject, "tenant", acc.TenantID)
		return nil
	}

	slog.Info("Matched subject rule, downloading body...", "subject", header.Subject)

	bodyURL := fmt.Sprintf("https://graph.microsoft.com/v1.0/users/%s/messages/%s?$select=body", url.PathEscape(payload.Email), url.PathEscape(payload.MessageID))
	bodyReq, err := http.NewRequestWithContext(ctx, "GET", bodyURL, nil)
	if err != nil {
		return err
	}
	bodyReq.Header.Set("Authorization", "Bearer "+accessToken)

	bodyResp, err := http.DefaultClient.Do(bodyReq)
	if err != nil {
		return fmt.Errorf("MS Graph body request failed: %w", err)
	}
	defer bodyResp.Body.Close()

	if bodyResp.StatusCode == http.StatusTooManyRequests {
		retryAfter := 10 // default
		if hValue := bodyResp.Header.Get("Retry-After"); hValue != "" {
			if seconds, err := strconv.Atoi(hValue); err == nil {
				retryAfter = seconds
			}
		}
		slog.Warn("Rate limited (429) by MS Graph body API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
		h.rescheduleFetchTask(ctx, t, retryAfter)
		return nil
	}

	if bodyResp.StatusCode == http.StatusUnauthorized || bodyResp.StatusCode == http.StatusForbidden {
		bodyBytes, _ := io.ReadAll(bodyResp.Body)
		slog.Error("Outlook Graph API body request unauthorized/forbidden, token revoked", "email", acc.Email, "status", bodyResp.Status, "body", string(bodyBytes))
		db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", fmt.Sprintf("Graph API body request failed: status %s, body %s", bodyResp.Status, string(bodyBytes)))
		h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
		return nil
	}
	if bodyResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(bodyResp.Body)
		return fmt.Errorf("failed to fetch email body, status: %s, body: %s", bodyResp.Status, string(bodyBytes))
	}

	var msgBody MSGraphMessageBody
	if err := json.NewDecoder(bodyResp.Body).Decode(&msgBody); err != nil {
		return err
	}

	processPayload := map[string]interface{}{
		"tenant_id":  acc.TenantID,
		"account_id": acc.ID,
		"from":       acc.Email, // Store mailbox recipient email address as "from_email"
		"subject":    header.Subject,
		"date":       header.ReceivedDateTime.Format(time.RFC3339), // Store true received date
		"body_text":  msgBody.Body.Content,
	}

	processBytes, err := json.Marshal(processPayload)
	if err != nil {
		return err
	}

	processTask := asynq.NewTask(TypeEmailProcess, processBytes, asynq.Queue("default"))
	_, err = h.queueClient.AsynqClient.Enqueue(processTask)
	if err != nil {
		slog.Error("Failed to enqueue email process task", "error", err)
		return fmt.Errorf("enqueue process task failed: %w", err)
	}

	slog.Info("Successfully enqueued email process task", "email", payload.Email, "subject", header.Subject)
	return nil
}

type ResendReceivedEmail struct {
	ID        string    `json:"id"`
	To        []string  `json:"to"`
	From      string    `json:"from"`
	CreatedAt time.Time `json:"created_at"`
	Subject   string    `json:"subject"`
	HTML      string    `json:"html"`
	Text      *string   `json:"text"`
}

func (h *TaskHandler) HandleResendFetch(ctx context.Context, t *asynq.Task, payload FetchTaskPayload) error {
	acc, err := db.GetEmailAccountByEmailAndProvider(ctx, h.dbPool, payload.Email, "resend")
	if err != nil {
		return fmt.Errorf("db fetch email account failed: %w", err)
	}
	if acc == nil {
		slog.Warn("Active Resend email account not found in database", "email", payload.Email)
		return nil
	}

	// Deduplication check
	isDup, err := h.isDuplicateMessage(ctx, payload.MessageID)
	if err != nil {
		slog.Error("Deduplication check failed", "error", err, "message_id", payload.MessageID)
	} else if isDup {
		slog.Info("Resend message already processed (duplicate), skipping", "message_id", payload.MessageID, "email", payload.Email)
		return nil
	}

	var creds struct {
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		return fmt.Errorf("failed to parse credentials JSON: %w", err)
	}

	// Fetch received email from Resend API
	reqURL := fmt.Sprintf("https://api.resend.com/emails/receiving/%s", payload.MessageID)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+creds.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Resend receiving email API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		bodyBytes, _ := io.ReadAll(resp.Body)
		slog.Error("Resend API request unauthorized/forbidden, API key revoked", "email", acc.Email, "status", resp.Status, "body", string(bodyBytes))
		db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", fmt.Sprintf("Resend API failed: status %s, body %s", resp.Status, string(bodyBytes)))
		h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
		return nil
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := 10 // default
		if hValue := resp.Header.Get("Retry-After"); hValue != "" {
			if seconds, err := strconv.Atoi(hValue); err == nil {
				retryAfter = seconds
			}
		}
		slog.Warn("Rate limited (429) by Resend API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
		h.rescheduleFetchTask(ctx, t, retryAfter)
		return nil
	}

	if resp.StatusCode == http.StatusNotFound {
		slog.Warn("Message not found in Resend", "message_id", payload.MessageID)
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to fetch received email, status: %s, body: %s", resp.Status, string(bodyBytes))
	}

	var emailMsg ResendReceivedEmail
	if err := json.NewDecoder(resp.Body).Decode(&emailMsg); err != nil {
		return err
	}

	// Compute time window filter start: max(LastSyncAt, now - 10 minutes)
	now := time.Now()
	tenMinutesAgo := now.Add(-10 * time.Minute)
	filterStart := tenMinutesAgo
	if acc.LastSyncAt != nil && acc.LastSyncAt.After(tenMinutesAgo) {
		filterStart = *acc.LastSyncAt
	}

	if emailMsg.CreatedAt.Before(filterStart) {
		slog.Info("Resend message is older than filter start time, skipping", "message_id", payload.MessageID, "created_at", emailMsg.CreatedAt, "filter_start", filterStart, "email", acc.Email)
		return nil
	}

	rules, err := h.ruleCache.GetRules(ctx, acc.TenantID)
	if err != nil {
		return fmt.Errorf("failed to fetch subject rules: %w", err)
	}

	var matchedRule *model.SubjectRule
	for _, rule := range rules {
		if strings.EqualFold(strings.TrimSpace(emailMsg.Subject), strings.TrimSpace(rule.Subject)) {
			matchedRule = &rule
			break
		}
	}

	if matchedRule == nil {
		slog.Info("Resend email subject did not match any tenant rules, discarding", "subject", emailMsg.Subject, "tenant", acc.TenantID)
		return nil
	}

	slog.Info("Matched subject rule for Resend, processing email...", "subject", emailMsg.Subject)

	// Extract body text
	bodyText := emailMsg.HTML
	if emailMsg.Text != nil && *emailMsg.Text != "" {
		bodyText = *emailMsg.Text
	}

	processPayload := map[string]interface{}{
		"tenant_id":  acc.TenantID,
		"account_id": acc.ID,
		"from":       acc.Email,
		"subject":    emailMsg.Subject,
		"date":       emailMsg.CreatedAt.Format(time.RFC3339),
		"body_text":  bodyText,
	}

	processBytes, err := json.Marshal(processPayload)
	if err != nil {
		return err
	}

	processTask := asynq.NewTask(TypeEmailProcess, processBytes, asynq.Queue("default"))
	_, err = h.queueClient.AsynqClient.Enqueue(processTask)
	if err != nil {
		slog.Error("Failed to enqueue Resend email process task", "error", err)
		return fmt.Errorf("enqueue process task failed: %w", err)
	}

	slog.Info("Successfully enqueued Resend email process task", "email", payload.Email, "subject", emailMsg.Subject)
	return nil
}

func (h *TaskHandler) HandleGmailFetch(ctx context.Context, t *asynq.Task, payload FetchTaskPayload) error {
	acc, err := db.GetEmailAccountByEmailAndProvider(ctx, h.dbPool, payload.Email, "gmail")
	if err != nil {
		return fmt.Errorf("db fetch email account failed: %w", err)
	}
	if acc == nil {
		slog.Warn("Active Gmail account not found in database", "email", payload.Email)
		return nil
	}

	if acc.GCPProjectID == nil {
		return fmt.Errorf("gmail account %s has no allocated GCP project ID", acc.Email)
	}

	clientID, clientSecret, err := db.GetGCPProjectCredentials(ctx, h.dbPool, *acc.GCPProjectID)
	if err != nil {
		return fmt.Errorf("failed to fetch GCP project credentials: %w", err)
	}

	var creds model.GmailCredentials
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		return fmt.Errorf("failed to parse credentials JSON: %w", err)
	}

	accessToken := creds.AccessToken
	if time.Now().Add(5 * time.Minute).After(creds.Expiry) {
		slog.Info("Gmail access token expired or expiring soon, refreshing", "email", acc.Email)
		newTokens, err := h.refreshGmailToken(ctx, clientID, clientSecret, creds.RefreshToken)
		if err != nil {
			slog.Error("Failed to refresh Gmail access token", "error", err, "email", acc.Email)
			db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", "OAuth token refresh failed: "+err.Error())
			h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
			return err
		}
		creds.AccessToken = newTokens.AccessToken
		creds.Expiry = newTokens.Expiry

		credsBytes, _ := json.Marshal(creds)
		if err := db.UpdateEmailAccountCredentialsAndStatus(ctx, h.dbPool, acc.ID, string(credsBytes), "ACTIVE"); err != nil {
			slog.Error("Failed to save refreshed credentials", "error", err)
		}
		accessToken = creds.AccessToken
	}

	var messageIDs []string
	var latestHistoryIDStr string

	// 1. Fetch message IDs added since last sync using history.list
	if creds.LastHistoryID != "" && creds.LastHistoryID != "0" && payload.HistoryID > 0 {
		historyURL := fmt.Sprintf("https://gmail.googleapis.com/gmail/v1/users/me/history?startHistoryId=%s", creds.LastHistoryID)
		req, _ := http.NewRequestWithContext(ctx, "GET", historyURL, nil)
		req.Header.Set("Authorization", "Bearer "+accessToken)

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusTooManyRequests {
				retryAfter := 15
				if hValue := resp.Header.Get("Retry-After"); hValue != "" {
					if seconds, err := strconv.Atoi(hValue); err == nil {
						retryAfter = seconds
					}
				}
				slog.Warn("Rate limited (429) by Gmail History API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
				h.rescheduleFetchTask(ctx, t, retryAfter)
				resp.Body.Close()
				return nil
			}

			var histResp GmailHistoryResponse
			if err := json.NewDecoder(resp.Body).Decode(&histResp); err == nil {
				for _, hist := range histResp.History {
					if hist.ID > latestHistoryIDStr {
						latestHistoryIDStr = hist.ID
					}
					for _, msgAdded := range hist.MessagesAdded {
						messageIDs = append(messageIDs, msgAdded.Message.ID)
					}
				}
			}
			resp.Body.Close()
		} else {
			if resp != nil {
				resp.Body.Close()
			}
			slog.Warn("History API call failed or history expired, falling back to messages.list", "email", acc.Email)
		}
	}

	// Compute time window filter start: max(LastSyncAt, now - 10 minutes)
	now := time.Now()
	tenMinutesAgo := now.Add(-10 * time.Minute)
	filterStart := tenMinutesAgo
	if acc.LastSyncAt != nil && acc.LastSyncAt.After(tenMinutesAgo) {
		filterStart = *acc.LastSyncAt
	}

	// 2. Fallback: list messages from the filterStart time window
	if len(messageIDs) == 0 {
		listURL := fmt.Sprintf("https://gmail.googleapis.com/gmail/v1/users/me/messages?q=after:%d", filterStart.Unix())
		req, _ := http.NewRequestWithContext(ctx, "GET", listURL, nil)
		req.Header.Set("Authorization", "Bearer "+accessToken)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to list fallback Gmail messages: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := 15
			if hValue := resp.Header.Get("Retry-After"); hValue != "" {
				if seconds, err := strconv.Atoi(hValue); err == nil {
					retryAfter = seconds
				}
			}
			slog.Warn("Rate limited (429) by Gmail List API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
			h.rescheduleFetchTask(ctx, t, retryAfter)
			return nil
		}

		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			bodyBytes, _ := io.ReadAll(resp.Body)
			slog.Error("Gmail List API request unauthorized/forbidden, token revoked", "email", acc.Email, "status", resp.Status, "body", string(bodyBytes))
			db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", fmt.Sprintf("Gmail List API failed: status %s, body %s", resp.Status, string(bodyBytes)))
			h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
			return nil
		}

		if resp.StatusCode == http.StatusOK {
			var listResp GmailListResponse
			if err := json.NewDecoder(resp.Body).Decode(&listResp); err == nil {
				for _, msg := range listResp.Messages {
					messageIDs = append(messageIDs, msg.ID)
				}
			}
		}
	}

	if len(messageIDs) == 0 {
		slog.Info("No new Gmail messages found", "email", acc.Email)
		return nil
	}

	// Deduplicate message IDs
	msgIDMap := make(map[string]bool)
	var uniqueMsgIDs []string
	for _, id := range messageIDs {
		if !msgIDMap[id] {
			msgIDMap[id] = true
			uniqueMsgIDs = append(uniqueMsgIDs, id)
		}
	}

	rules, err := h.ruleCache.GetRules(ctx, acc.TenantID)
	if err != nil {
		return fmt.Errorf("failed to fetch subject rules: %w", err)
	}

	// 3. Process message headers and check rules (Header-Only Pre-Filtering)
	for _, msgID := range uniqueMsgIDs {
		// Deduplication check
		isDup, err := h.isDuplicateMessage(ctx, msgID)
		if err != nil {
			slog.Error("Deduplication check failed", "error", err, "message_id", msgID)
		} else if isDup {
			slog.Info("Gmail message already processed (duplicate), skipping", "message_id", msgID, "email", payload.Email)
			continue
		}

		metaURL := fmt.Sprintf("https://gmail.googleapis.com/gmail/v1/users/me/messages/%s?format=metadata&metadataHeaders=Subject&metadataHeaders=From", msgID)
		req, _ := http.NewRequestWithContext(ctx, "GET", metaURL, nil)
		req.Header.Set("Authorization", "Bearer "+accessToken)

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusTooManyRequests {
				retryAfter := 15
				if hValue := resp.Header.Get("Retry-After"); hValue != "" {
					if seconds, err := strconv.Atoi(hValue); err == nil {
						retryAfter = seconds
					}
				}
				slog.Warn("Rate limited (429) by Gmail Metadata API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
				h.rescheduleFetchTask(ctx, t, retryAfter)
				resp.Body.Close()
				return nil
			}

			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				bodyBytes, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				slog.Error("Gmail Metadata API request unauthorized/forbidden, token revoked", "email", acc.Email, "status", resp.Status, "body", string(bodyBytes))
				db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", fmt.Sprintf("Gmail Metadata API failed: status %s, body %s", resp.Status, string(bodyBytes)))
				h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
				return nil
			}
			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				continue
			}
		} else {
			continue
		}

		var meta GmailMessageMetadata
		json.NewDecoder(resp.Body).Decode(&meta)
		resp.Body.Close()

		ms, _ := strconv.ParseInt(meta.InternalDate, 10, 64)
		receivedTime := time.Unix(0, ms*int64(time.Millisecond))

		if receivedTime.Before(filterStart) {
			slog.Info("Gmail message is older than filter start time, skipping", "message_id", msgID, "received", receivedTime, "filter_start", filterStart, "email", acc.Email)
			continue
		}

		if meta.HistoryID > latestHistoryIDStr {
			latestHistoryIDStr = meta.HistoryID
		}

		var subject string
		for _, header := range meta.Payload.Headers {
			if strings.EqualFold(header.Name, "Subject") {
				subject = header.Value
			}
		}

		var matchedRule *model.SubjectRule
		for _, rule := range rules {
			if strings.EqualFold(strings.TrimSpace(subject), strings.TrimSpace(rule.Subject)) {
				matchedRule = &rule
				break
			}
		}

		if matchedRule == nil {
			continue
		}

		slog.Info("Gmail matched subject rule, downloading body...", "msg_id", msgID, "subject", subject)

		// 4. Full body fetch (Attachment Bypass)
		fullURL := fmt.Sprintf("https://gmail.googleapis.com/gmail/v1/users/me/messages/%s?format=full", msgID)
		fullReq, _ := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		fullReq.Header.Set("Authorization", "Bearer "+accessToken)

		fullResp, err := http.DefaultClient.Do(fullReq)
		if err == nil {
			if fullResp.StatusCode == http.StatusTooManyRequests {
				retryAfter := 15
				if hValue := fullResp.Header.Get("Retry-After"); hValue != "" {
					if seconds, err := strconv.Atoi(hValue); err == nil {
						retryAfter = seconds
					}
				}
				slog.Warn("Rate limited (429) by Gmail Full Body API, rescheduling task", "email", payload.Email, "retry_after_seconds", retryAfter)
				h.rescheduleFetchTask(ctx, t, retryAfter)
				fullResp.Body.Close()
				return nil
			}

			if fullResp.StatusCode == http.StatusUnauthorized || fullResp.StatusCode == http.StatusForbidden {
				bodyBytes, _ := io.ReadAll(fullResp.Body)
				fullResp.Body.Close()
				slog.Error("Gmail Full Body API request unauthorized/forbidden, token revoked", "email", acc.Email, "status", fullResp.Status, "body", string(bodyBytes))
				db.UpdateEmailAccountStatus(ctx, h.dbPool, acc.ID, "REAUTH_REQUIRED", fmt.Sprintf("Gmail Full Body API failed: status %s, body %s", fullResp.Status, string(bodyBytes)))
				h.publishStatusChange(ctx, acc.TenantID, acc.Email, "REAUTH_REQUIRED")
				return nil
			}
			if fullResp.StatusCode != http.StatusOK {
				fullResp.Body.Close()
				continue
			}
		} else {
			continue
		}

		var fullMsg GmailMessageFull
		json.NewDecoder(fullResp.Body).Decode(&fullMsg)
		fullResp.Body.Close()

		bodyContent := extractGmailBody(fullMsg.Payload)

		// 5. Enqueue TypeEmailProcess task
		processPayload := map[string]interface{}{
			"tenant_id":  acc.TenantID,
			"account_id": acc.ID,
			"from":       acc.Email, // Store mailbox recipient email address as "from_email"
			"subject":    subject,
			"date":       receivedTime.Format(time.RFC3339), // Store true received date
			"body_text":  bodyContent,
		}
		processBytes, _ := json.Marshal(processPayload)

		processTask := asynq.NewTask(TypeEmailProcess, processBytes, asynq.Queue("default"))
		_, err = h.queueClient.AsynqClient.Enqueue(processTask)
		if err != nil {
			slog.Error("Failed to enqueue email process task", "error", err)
		}
	}

	// Update latest history ID in database
	if latestHistoryIDStr != "" && latestHistoryIDStr != creds.LastHistoryID {
		creds.LastHistoryID = latestHistoryIDStr
		credsBytes, _ := json.Marshal(creds)
		db.UpdateEmailAccountCredentialsAndStatus(ctx, h.dbPool, acc.ID, string(credsBytes), "ACTIVE")
	}

	return nil
}

func (h *TaskHandler) HandleIMAPFetch(ctx context.Context, t *asynq.Task, payload FetchTaskPayload) error {
	acc, err := db.GetEmailAccountByEmailAndProvider(ctx, h.dbPool, payload.Email, "imap")
	if err != nil {
		return fmt.Errorf("failed to fetch IMAP account: %w", err)
	}
	if acc == nil {
		slog.Warn("Active IMAP account not found in database", "email", payload.Email)
		return nil
	}

	var creds model.IMAPCredentials
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		return fmt.Errorf("failed to parse IMAP credentials: %w", err)
	}

	addr := fmt.Sprintf("%s:%d", creds.Host, creds.Port)
	var c *client.Client

	if creds.Security == "ssl" {
		c, err = client.DialTLS(addr, &tls.Config{InsecureSkipVerify: true})
	} else {
		c, err = client.Dial(addr)
		if err == nil && creds.Security == "starttls" {
			err = c.StartTLS(&tls.Config{InsecureSkipVerify: true})
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to IMAP server: %w", err)
	}
	defer c.Logout()

	if err := c.Login(creds.Username, creds.Password); err != nil {
		return fmt.Errorf("failed to authenticate with IMAP server: %w", err)
	}

	mbox, err := c.Select("INBOX", true) // Open as read-only
	if err != nil {
		return fmt.Errorf("failed to select INBOX: %w", err)
	}

	if mbox.Messages == 0 {
		slog.Info("IMAP mailbox INBOX is empty", "email", acc.Email)
		return nil
	}

	// 1. Fetch sequence set for the last 10 messages
	seqset := new(imap.SeqSet)
	start := mbox.Messages - 9
	if start < 1 {
		start = 1
	}
	seqset.AddRange(start, mbox.Messages)

	items := []imap.FetchItem{imap.FetchEnvelope, imap.FetchBodyStructure}
	messagesChan := make(chan *imap.Message, 10)
	doneChan := make(chan error, 1)

	go func() {
		doneChan <- c.Fetch(seqset, items, messagesChan)
	}()

	var messages []*imap.Message
	for msg := range messagesChan {
		messages = append(messages, msg)
	}

	if err := <-doneChan; err != nil {
		return fmt.Errorf("failed to fetch IMAP message envelopes: %w", err)
	}

	rules, err := h.ruleCache.GetRules(ctx, acc.TenantID)
	if err != nil {
		return fmt.Errorf("failed to load tenant subject rules: %w", err)
	}

	// Compute time window filter start: max(LastSyncAt, now - 10 minutes)
	now := time.Now()
	tenMinutesAgo := now.Add(-10 * time.Minute)
	filterStart := tenMinutesAgo
	if acc.LastSyncAt != nil && acc.LastSyncAt.After(tenMinutesAgo) {
		filterStart = *acc.LastSyncAt
	}

	// 2. Filter messages and fetch matching bodies (Header-Only Pre-Filtering + Attachment Bypass)
	for _, msg := range messages {
		if msg.Envelope == nil {
			continue
		}

		// Deduplication check
		messageID := msg.Envelope.MessageId
		if messageID == "" {
			messageID = fmt.Sprintf("%s:%d", acc.Email, msg.Uid)
		}
		isDup, err := h.isDuplicateMessage(ctx, messageID)
		if err != nil {
			slog.Error("Deduplication check failed", "error", err, "message_id", messageID)
		} else if isDup {
			slog.Info("IMAP message already processed (duplicate), skipping", "message_id", messageID, "email", acc.Email)
			continue
		}

		if msg.Envelope.Date.Before(filterStart) {
			slog.Info("IMAP message is older than filter start time, skipping", "email", acc.Email, "received", msg.Envelope.Date, "filter_start", filterStart)
			continue
		}

		subject := msg.Envelope.Subject

		// Match rules
		var matchedRule *model.SubjectRule
		for _, rule := range rules {
			if strings.EqualFold(strings.TrimSpace(subject), strings.TrimSpace(rule.Subject)) {
				matchedRule = &rule
				break
			}
		}

		if matchedRule == nil {
			continue
		}

		slog.Info("IMAP matched subject rule, downloading text body part", "email", acc.Email, "subject", subject)

		// Determine section path of the text body part (Attachment Bypass)
		section := &imap.BodySectionName{}
		if partNum := findTextPart(msg.BodyStructure, ""); partNum != "" {
			section.Path = parsePath(partNum)
		}

		bodySeqset := new(imap.SeqSet)
		bodySeqset.AddNum(msg.SeqNum)

		bodyChan := make(chan *imap.Message, 1)
		bodyDone := make(chan error, 1)

		go func() {
			bodyDone <- c.Fetch(bodySeqset, []imap.FetchItem{section.FetchItem()}, bodyChan)
		}()

		var bodyMsg *imap.Message
		for m := range bodyChan {
			bodyMsg = m
		}

		if err := <-bodyDone; err != nil || bodyMsg == nil {
			slog.Error("Failed to fetch IMAP message body part", "seqNum", msg.SeqNum, "error", err)
			continue
		}

		r := bodyMsg.GetBody(section)
		if r == nil {
			continue
		}

		bodyBytes, err := io.ReadAll(r)
		if err != nil {
			continue
		}

		// Enqueue TypeEmailProcess task
		processPayload := map[string]interface{}{
			"tenant_id":  acc.TenantID,
			"account_id": acc.ID,
			"from":       acc.Email, // Store mailbox recipient email address as "from_email"
			"subject":    subject,
			"date":       msg.Envelope.Date.Format(time.RFC3339), // Store true received date
			"body_text":  string(bodyBytes),
		}
		processBytes, _ := json.Marshal(processPayload)

		processTask := asynq.NewTask(TypeEmailProcess, processBytes, asynq.Queue("default"))
		_, err = h.queueClient.AsynqClient.Enqueue(processTask)
		if err != nil {
			slog.Error("Failed to enqueue IMAP email process task", "error", err)
		}
	}

	return nil
}

func (h *TaskHandler) HandleEmailProcessTask(ctx context.Context, t *asynq.Task) error {
	var payload ProcessTaskPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		slog.Error("Failed to unmarshal process task payload", "error", err)
		return fmt.Errorf("invalid payload: %w", err)
	}

	slog.Info("Running email process task", "tenant", payload.TenantID, "from", payload.From, "subject", payload.Subject)

	// 1. Fetch cached rules
	rules, err := h.ruleCache.GetRules(ctx, payload.TenantID)
	if err != nil {
		return fmt.Errorf("failed to fetch rules for tenant: %w", err)
	}

	// 2. Match subject rule
	var matchedRule *model.SubjectRule
	for _, rule := range rules {
		if strings.EqualFold(strings.TrimSpace(payload.Subject), strings.TrimSpace(rule.Subject)) {
			matchedRule = &rule
			break
		}
	}

	if matchedRule == nil {
		slog.Info("Processed email did not match any tenant rules, discarding", "subject", payload.Subject, "tenant", payload.TenantID)
		return nil
	}
	// 3. Select body for extraction: raw HTML/Text is needed for URLs/RAW to preserve content.
	var targetBody string
	if matchedRule.ExtractMethod == "NETFLIX_URL_EXTRACT" || matchedRule.ExtractMethod == "RAW" {
		targetBody = payload.BodyText
	} else {
		// Clean and normalize text for OTP pattern matching
		targetBody = parser.NormalizeHTML(payload.BodyText)
	}

	// 4. Extract data using regex pattern
	extractedData, err := parser.ExtractData(targetBody, matchedRule.ExtractMethod)
	if err != nil {
		slog.Warn("Failed to extract data from normalized email body", "subject", payload.Subject, "method", matchedRule.ExtractMethod, "error", err)
		return nil // Return nil so we don't retry a non-matching email structure
	}

	slog.Info("Successfully extracted data from email", "data", extractedData)

	// 5. Write directly to tenant TimescaleDB hypertable
	query := fmt.Sprintf(`
		INSERT INTO "%s".email_message_ts (tenant_id, from_email, subject, email_date, parsed_data, extract_method, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`, payload.TenantID)

	parsedDate, err := time.Parse(time.RFC3339, payload.Date)
	if err != nil {
		parsedDate = time.Now()
	}

	_, err = h.dbPool.Exec(ctx, query, payload.TenantID, payload.From, payload.Subject, parsedDate, extractedData, matchedRule.ExtractMethod)
	if err != nil {
		slog.Error("Failed to insert parsed email message into tenant hypertable", "tenant", payload.TenantID, "error", err)
		return fmt.Errorf("database insert failed: %w", err)
	}

	// Update last_sync_at to the date of the processed email (only if it is newer)
	_, err = h.dbPool.Exec(ctx, `
		UPDATE master.email_accounts
		SET last_sync_at = $1
		WHERE id = $2 AND (last_sync_at IS NULL OR last_sync_at < $1)
	`, parsedDate, payload.AccountID)
	if err != nil {
		slog.Error("Failed to update last_sync_at for email account", "account", payload.AccountID, "error", err)
	}

	// 6. Broadcast push notification to Redis Pub/Sub to trigger monolith Websocket rooms
	broadcastChannel := "email_events:broadcast"
	replacer := strings.NewReplacer(".", "_", "@", "_")
	sanitizedFrom := replacer.Replace(strings.ToLower(payload.From))

	eventPayload := map[string]string{
		"tenant_id":      payload.TenantID,
		"from":           sanitizedFrom,
		"date":           payload.Date,
		"subject":        payload.Subject,
		"extract_method": matchedRule.ExtractMethod,
		"data":           extractedData,
	}
	eventBytes, _ := json.Marshal(eventPayload)
	h.queueClient.RedisClient.Publish(ctx, broadcastChannel, string(eventBytes))

	slog.Info("Successfully wrote parsed email to DB and broadcasted event", "tenant", payload.TenantID)
	return nil
}

func (h *TaskHandler) refreshOutlookToken(ctx context.Context, refreshToken string) (*model.OutlookCredentials, error) {
	tokenURL := "https://login.microsoftonline.com/common/oauth2/v2.0/token"
	formData := url.Values{}
	formData.Set("client_id", h.cfg.OutlookClientID)
	formData.Set("client_secret", h.cfg.OutlookClientSecret)
	formData.Set("refresh_token", refreshToken)
	formData.Set("grant_type", "refresh_token")

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("refresh rejected: status %s, body %s", resp.Status, string(respBytes))
	}

	var refreshResp OutlookRefreshResponse
	if err := json.NewDecoder(resp.Body).Decode(&refreshResp); err != nil {
		return nil, err
	}

	return &model.OutlookCredentials{
		AccessToken:  refreshResp.AccessToken,
		RefreshToken: refreshResp.RefreshToken,
		TokenType:    refreshResp.TokenType,
		Expiry:       time.Now().Add(time.Duration(refreshResp.ExpiresIn) * time.Second),
	}, nil
}

func (h *TaskHandler) refreshGmailToken(ctx context.Context, clientID, clientSecret, refreshToken string) (*model.GmailCredentials, error) {
	tokenURL := "https://oauth2.googleapis.com/token"
	formData := url.Values{}
	formData.Set("client_id", clientID)
	formData.Set("client_secret", clientSecret)
	formData.Set("refresh_token", refreshToken)
	formData.Set("grant_type", "refresh_token")

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gmail refresh rejected: status %s, body %s", resp.Status, string(respBytes))
	}

	var refreshResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&refreshResp); err != nil {
		return nil, err
	}

	return &model.GmailCredentials{
		AccessToken:  refreshResp.AccessToken,
		RefreshToken: refreshToken, // Refresh token stays the same
		TokenType:    refreshResp.TokenType,
		Expiry:       time.Now().Add(time.Duration(refreshResp.ExpiresIn) * time.Second),
	}, nil
}

func extractGmailBody(part GmailPart) string {
	if part.Body.Data != "" && part.Body.AttachmentID == "" {
		decoded, err := base64.URLEncoding.DecodeString(part.Body.Data)
		if err == nil {
			return string(decoded)
		}
	}

	var bodyText string
	for _, p := range part.Parts {
		text := extractGmailBody(p)
		if text != "" {
			if p.MimeType == "text/html" {
				return text
			}
			bodyText = text
		}
	}
	return bodyText
}

func findTextPart(bs *imap.BodyStructure, prefix string) string {
	if bs == nil {
		return ""
	}
	if strings.ToLower(bs.MIMEType) == "text" {
		return prefix
	}
	for i, part := range bs.Parts {
		var newPrefix string
		if prefix == "" {
			newPrefix = fmt.Sprintf("%d", i+1)
		} else {
			newPrefix = fmt.Sprintf("%s.%d", prefix, i+1)
		}
		if p := findTextPart(part, newPrefix); p != "" {
			// Prefer HTML if it is available
			if strings.ToLower(part.MIMESubType) == "html" {
				return p
			}
			return p
		}
	}
	return ""
}

func parsePath(pathStr string) []int {
	parts := strings.Split(pathStr, ".")
	var path []int
	for _, p := range parts {
		var val int
		fmt.Sscanf(p, "%d", &val)
		path = append(path, val)
	}
	return path
}

type EmailDisconnectPayload struct {
	EmailAccountID string `json:"email_account_id"`
}

func (h *TaskHandler) HandleEmailDisconnectTask(ctx context.Context, t *asynq.Task) error {
	var payload EmailDisconnectPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("failed to unmarshal disconnect payload: %w", err)
	}

	slog.Info("Processing email disconnect task", "email_account_id", payload.EmailAccountID)

	// 1. Fetch the account from database
	acc, err := db.GetEmailAccountByID(ctx, h.dbPool, payload.EmailAccountID)
	if err != nil {
		return fmt.Errorf("failed to fetch email account: %w", err)
	}
	if acc == nil {
		slog.Warn("Email account not found for disconnect", "id", payload.EmailAccountID)
		return nil
	}

	// 2. Perform provider-specific cleanup
	switch acc.Provider {
	case "gmail":
		h.cleanupGmailConnection(ctx, acc)
	case "outlook":
		h.cleanupOutlookConnection(ctx, acc)
	}

	// 3. Clear database credentials & set status to DISCONNECTED
	var dbErr error
	if acc.Provider == "gmail" {
		_, dbErr = h.dbPool.Exec(ctx, `
			UPDATE master.email_accounts
			SET status = 'DISCONNECTED', credentials = '', gcp_project_id = NULL, last_error = NULL, updated_at = NOW()
			WHERE id = $1
		`, acc.ID)
	} else {
		_, dbErr = h.dbPool.Exec(ctx, `
			UPDATE master.email_accounts
			SET status = 'DISCONNECTED', credentials = '', last_error = NULL, updated_at = NOW()
			WHERE id = $1
		`, acc.ID)
	}

	if dbErr != nil {
		return fmt.Errorf("failed to clear email account credentials in DB: %w", dbErr)
	}

	// 4. Broadcast connection-disconnected event via Redis pub/sub
	h.broadcastDisconnection(ctx, acc)

	slog.Info("Successfully disconnected and cleaned up email account", "email", acc.Email, "id", acc.ID)
	return nil
}

func (h *TaskHandler) cleanupGmailConnection(ctx context.Context, acc *model.EmailAccount) {
	slog.Info("Cleaning up Gmail connection", "email", acc.Email)

	var creds model.GmailCredentials
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		slog.Warn("failed to parse Gmail credentials for cleanup", "email", acc.Email, "err", err)
		return
	}

	if acc.GCPProjectID == nil {
		slog.Warn("Gmail account has no associated GCP project ID", "email", acc.Email)
		return
	}

	// 1. Fetch GCP project credentials
	clientID, clientSecret, err := db.GetGCPProjectCredentials(ctx, h.dbPool, *acc.GCPProjectID)
	if err != nil {
		slog.Error("failed to fetch GCP project credentials for cleanup", "email", acc.Email, "err", err)
		return
	}

	// 2. Refresh token if expired
	accessToken := creds.AccessToken
	if time.Now().After(creds.Expiry) {
		newCreds, err := h.refreshGmailToken(ctx, clientID, clientSecret, creds.RefreshToken)
		if err != nil {
			slog.Error("failed to refresh Gmail token for cleanup", "email", acc.Email, "err", err)
		} else {
			accessToken = newCreds.AccessToken
		}
	}

	// 3. Call stop watch API
	if accessToken != "" {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://gmail.googleapis.com/gmail/v1/users/me/stop", nil)
		if err == nil {
			req.Header.Set("Authorization", "Bearer "+accessToken)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
				slog.Info("Successfully stopped Gmail watch webhook", "email", acc.Email)
			} else {
				slog.Warn("Gmail stop watch API request failed", "email", acc.Email, "err", err)
			}
		}
	}

	// 4. Call OAuth Revoke API
	if creds.RefreshToken != "" {
		revokeURL := fmt.Sprintf("https://oauth2.googleapis.com/revoke?token=%s", url.QueryEscape(creds.RefreshToken))
		resp, err := http.Post(revokeURL, "application/x-www-form-urlencoded", nil)
		if err == nil {
			resp.Body.Close()
			slog.Info("Successfully revoked Google OAuth token", "email", acc.Email)
		} else {
			slog.Warn("Google token revocation request failed", "email", acc.Email, "err", err)
		}
	}

	// 5. Decrement active_count in master.gcp_projects
	_, err = h.dbPool.Exec(ctx, `
		UPDATE master.gcp_projects
		SET active_count = GREATEST(0, active_count - 1), updated_at = NOW()
		WHERE id = $1
	`, *acc.GCPProjectID)
	if err != nil {
		slog.Error("failed to decrement GCP project active_count", "project_id", *acc.GCPProjectID, "err", err)
	}
}

func (h *TaskHandler) cleanupOutlookConnection(ctx context.Context, acc *model.EmailAccount) {
	slog.Info("Cleaning up Outlook connection", "email", acc.Email)

	var creds model.OutlookCredentials
	if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
		slog.Warn("failed to parse Outlook credentials for cleanup", "email", acc.Email, "err", err)
		return
	}

	if creds.SubscriptionID == "" {
		slog.Warn("Outlook account has no associated subscription ID", "email", acc.Email)
		return
	}

	// 1. Refresh token if expired
	accessToken := creds.AccessToken
	if time.Now().After(creds.Expiry) {
		newCreds, err := h.refreshOutlookToken(ctx, creds.RefreshToken)
		if err != nil {
			slog.Error("failed to refresh Outlook token for cleanup", "email", acc.Email, "err", err)
		} else {
			accessToken = newCreds.AccessToken
		}
	}

	// 2. Call DELETE subscription API
	if accessToken != "" {
		deleteURL := fmt.Sprintf("https://graph.microsoft.com/v1.0/subscriptions/%s", creds.SubscriptionID)
		req, err := http.NewRequestWithContext(ctx, "DELETE", deleteURL, nil)
		if err == nil {
			req.Header.Set("Authorization", "Bearer "+accessToken)
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				resp.Body.Close()
				slog.Info("Successfully deleted Microsoft Graph webhook subscription", "email", acc.Email, "sub_id", creds.SubscriptionID)
			} else {
				slog.Warn("Microsoft Graph delete subscription request failed", "email", acc.Email, "err", err)
			}
		}
	}
}

func (h *TaskHandler) broadcastDisconnection(ctx context.Context, acc *model.EmailAccount) {
	broadcastChannel := "email_events:broadcast"
	eventPayload := map[string]string{
		"tenant_id": acc.TenantID,
		"from":      acc.Email,
		"date":      time.Now().Format(time.RFC3339),
		"subject":   "System",
		"context":   "connection-disconnected",
		"data":      "disconnected",
	}
	eventBytes, _ := json.Marshal(eventPayload)
	h.queueClient.RedisClient.Publish(ctx, broadcastChannel, string(eventBytes))
}

func (h *TaskHandler) isDuplicateMessage(ctx context.Context, messageID string) (bool, error) {
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

func (h *TaskHandler) publishStatusChange(ctx context.Context, tenantID, email, status string) {
	broadcastChannel := "email_events:broadcast"
	replacer := strings.NewReplacer(".", "_", "@", "_")
	sanitizedEmail := replacer.Replace(strings.ToLower(email))

	eventPayload := map[string]string{
		"tenant_id": tenantID,
		"from":      sanitizedEmail,
		"date":      time.Now().Format(time.RFC3339),
		"subject":   "status_changed",
		"context":   "connection-status-changed",
		"data":      status,
	}
	eventBytes, _ := json.Marshal(eventPayload)
	h.queueClient.RedisClient.Publish(ctx, broadcastChannel, string(eventBytes))
}
