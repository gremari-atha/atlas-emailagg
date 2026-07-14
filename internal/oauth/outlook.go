package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"atlas-emailagg/internal/config"
	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/model"
	"atlas-emailagg/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// SetupOutlookRoutes registers OAuth endpoints for Microsoft/Outlook.
func SetupOutlookRoutes(r chi.Router, dbPool *pgxpool.Pool, qClient *queue.QueueClient, cfg *config.Config) {
	handler := &OutlookHandler{
		dbPool:      dbPool,
		queueClient: qClient,
		cfg:         cfg,
	}
	r.Get("/outlook/connect", handler.HandleConnect)
	r.Get("/outlook/callback", handler.HandleCallback)
}

type OutlookHandler struct {
	dbPool      *pgxpool.Pool
	queueClient *queue.QueueClient
	cfg         *config.Config
}

func (h *OutlookHandler) getRedirectURI(r *http.Request) string {
	if val := os.Getenv("OUTLOOK_REDIRECT_URI"); val != "" {
		return val
	}
	// Fallback to dynamic host detection
	scheme := "https"
	if strings.Contains(r.Host, "localhost") || strings.Contains(r.Host, "127.0.0.1") {
		scheme = "http"
	}
	return fmt.Sprintf("%s://%s/oauth/outlook/callback", scheme, r.Host)
}

func (h *OutlookHandler) HandleConnect(w http.ResponseWriter, r *http.Request) {
	emailID := r.URL.Query().Get("email_id")
	if emailID == "" {
		http.Error(w, "Missing email_id query parameter", http.StatusBadRequest)
		return
	}

	// Verify email account exists in master.email_accounts
	acc, err := db.GetEmailAccountByID(r.Context(), h.dbPool, emailID)
	if err != nil {
		slog.Error("Database query failed in Connect", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if acc == nil {
		http.Error(w, "Email account not registered", http.StatusNotFound)
		return
	}

	redirectURI := h.getRedirectURI(r)
	state := emailID // For simplicity, we pass the email UUID directly in state

	authURL := fmt.Sprintf(
		"https://login.microsoftonline.com/common/oauth2/v2.0/authorize?client_id=%s&response_type=code&redirect_uri=%s&response_mode=query&scope=%s&state=%s",
		url.QueryEscape(h.cfg.OutlookClientID),
		url.QueryEscape(redirectURI),
		url.QueryEscape("https://graph.microsoft.com/Mail.Read https://graph.microsoft.com/User.Read offline_access"),
		url.QueryEscape(state),
	)

	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

type MicrosoftTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

type MicrosoftProfileResponse struct {
	ID                string `json:"id"`
	UserPrincipalName string `json:"userPrincipalName"`
	Mail              string `json:"mail"`
}

func (h *OutlookHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state") // Contains our email account ID (UUID)

	if code == "" || state == "" {
		http.Error(w, "Missing code or state parameters", http.StatusBadRequest)
		return
	}

	// 1. Fetch registered email account
	acc, err := db.GetEmailAccountByID(r.Context(), h.dbPool, state)
	if err != nil {
		slog.Error("Failed to fetch email account in Callback", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if acc == nil {
		http.Error(w, "Email account not found", http.StatusNotFound)
		return
	}

	// 2. Exchange code for tokens
	redirectURI := h.getRedirectURI(r)
	tokenURL := "https://login.microsoftonline.com/common/oauth2/v2.0/token"
	formData := url.Values{}
	formData.Set("client_id", h.cfg.OutlookClientID)
	formData.Set("client_secret", h.cfg.OutlookClientSecret)
	formData.Set("code", code)
	formData.Set("redirect_uri", redirectURI)
	formData.Set("grant_type", "authorization_code")

	resp, err := http.PostForm(tokenURL, formData)
	if err != nil {
		slog.Error("Failed to POST Microsoft token exchange", "error", err)
		http.Error(w, "Token exchange connection failed", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		slog.Error("Microsoft token exchange rejected", "status", resp.Status, "body", string(bodyBytes))
		http.Error(w, "Microsoft token exchange rejected", http.StatusBadGateway)
		return
	}

	var tokenResp MicrosoftTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		slog.Error("Failed to decode Microsoft token response", "error", err)
		http.Error(w, "Failed to parse token response", http.StatusInternalServerError)
		return
	}

	// 3. Fetch Microsoft profile to verify matching email
	profileReq, err := http.NewRequestWithContext(r.Context(), "GET", "https://graph.microsoft.com/v1.0/me", nil)
	if err != nil {
		slog.Error("Failed to create profile request", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	profileReq.Header.Set("Authorization", "Bearer "+tokenResp.AccessToken)

	profileResp, err := http.DefaultClient.Do(profileReq)
	if err != nil {
		slog.Error("Failed to execute Microsoft profile request", "error", err)
		http.Error(w, "Failed to fetch Microsoft profile", http.StatusBadGateway)
		return
	}
	defer profileResp.Body.Close()

	if profileResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(profileResp.Body)
		slog.Error("Microsoft profile fetch failed", "status", profileResp.Status, "body", string(bodyBytes))
		http.Error(w, "Microsoft profile fetch failed", http.StatusBadGateway)
		return
	}

	var profile MicrosoftProfileResponse
	if err := json.NewDecoder(profileResp.Body).Decode(&profile); err != nil {
		slog.Error("Failed to decode Microsoft profile", "error", err)
		http.Error(w, "Failed to parse profile response", http.StatusInternalServerError)
		return
	}

	// Normalise email addresses
	authenticatedEmail := strings.ToLower(strings.TrimSpace(profile.Mail))
	if authenticatedEmail == "" {
		authenticatedEmail = strings.ToLower(strings.TrimSpace(profile.UserPrincipalName))
	}
	expectedEmail := strings.ToLower(strings.TrimSpace(acc.Email))

	if authenticatedEmail != expectedEmail {
		slog.Warn("Authenticated email mismatch", "expected", expectedEmail, "authenticated", authenticatedEmail)
		http.Error(w, fmt.Sprintf("Email mismatch: Authenticated with '%s' but expected '%s'", authenticatedEmail, expectedEmail), http.StatusForbidden)
		return
	}

	// 4. Register push notification subscription with Microsoft Graph
	notificationURL := fmt.Sprintf("https://%s/webhooks/outlook", r.Host)
	if val := os.Getenv("OUTLOOK_NOTIFICATION_URL"); val != "" {
		notificationURL = val
	}

	subID, subExpiry, err := RegisterOutlookSubscription(r.Context(), authenticatedEmail, tokenResp.AccessToken, notificationURL)
	if err != nil {
		slog.Error("Failed to register Outlook Graph subscription during OAuth flow", "error", err, "email", acc.Email)
		// We do not fail the overall connection flow, but log the failure so subscription loop can pick it up
	}

	// 4.5. Save credentials JSON in plaintext
	expiryTime := time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	creds := model.OutlookCredentials{
		AccessToken:     tokenResp.AccessToken,
		RefreshToken:    tokenResp.RefreshToken,
		TokenType:       tokenResp.TokenType,
		Expiry:          expiryTime,
		SubscriptionID:  subID,
		MicrosoftUserID: profile.ID,
	}

	credsBytes, err := json.Marshal(creds)
	if err != nil {
		slog.Error("Failed to marshal Outlook credentials", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	_, err = h.dbPool.Exec(r.Context(), `
		UPDATE master.email_accounts
		SET credentials = $1, status = 'ACTIVE', provider = 'outlook', last_sync_at = NOW(), last_error = NULL, updated_at = NOW()
		WHERE id = $2
	`, string(credsBytes), acc.ID)
	if err != nil {
		slog.Error("Failed to save credentials in database", "error", err)
		http.Error(w, "Internal database error", http.StatusInternalServerError)
		return
	}

	if subID != "" {
		slog.Info("Successfully registered Outlook Graph subscription", "sub_id", subID, "expiry", subExpiry)
		// Track in Redis ZSet
		h.queueClient.RedisClient.ZAdd(r.Context(), "zset:outlook_subscription_expirations", redis.Z{
			Score:  float64(subExpiry.Unix()),
			Member: acc.ID,
		})
	}


	// 5. Broadcast success event via Redis Pub/Sub to trigger websocket room update
	broadcastChannel := "email_events:broadcast"

	eventPayload := map[string]string{
		"tenant_id": acc.TenantID,
		"from":      acc.Email,
		"date":      time.Now().Format(time.RFC3339),
		"subject":   "System",
		"context":   "connection-success",
		"data":      "connected",
	}
	eventBytes, _ := json.Marshal(eventPayload)
	h.queueClient.RedisClient.Publish(r.Context(), broadcastChannel, string(eventBytes))

	slog.Info("Successfully linked Outlook account", "email", acc.Email, "tenant", acc.TenantID)

	// 6. Return connection success HTML page
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`
		<!DOCTYPE html>
		<html>
		<head>
			<title>Email Connected</title>
			<style>
				body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background-color: #f3f4f6; }
				.card { background: white; padding: 2.5rem; border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); text-align: center; max-width: 400px; }
				h1 { color: #10b981; margin-top: 0; font-size: 1.75rem; }
				p { color: #4b5563; font-size: 1rem; line-height: 1.5; }
			</style>
		</head>
		<body>
			<div class="card">
				<h1>Koneksi Berhasil!</h1>
				<p>Email <strong>` + acc.Email + `</strong> telah berhasil dihubungkan ke agregator Atlas. Anda dapat menutup tab ini sekarang.</p>
			</div>
		</body>
		</html>
	`))
}

type MicrosoftSubscriptionRequest struct {
	ChangeType         string `json:"changeType"`
	NotificationUrl    string `json:"notificationUrl"`
	Resource           string `json:"resource"`
	ExpirationDateTime string `json:"expirationDateTime"`
	ClientState        string `json:"clientState"`
}

type MicrosoftSubscriptionResponse struct {
	ID                 string `json:"id"`
	ExpirationDateTime string `json:"expirationDateTime"`
}

// RegisterOutlookSubscription calls MS Graph to create a push subscription
func RegisterOutlookSubscription(ctx context.Context, email, accessToken, notificationURL string) (string, time.Time, error) {
	// Expiry limit: 4230 minutes max. Let's register for 4000 minutes (2.7 days)
	expiration := time.Now().Add(4000 * time.Minute)

	reqPayload := MicrosoftSubscriptionRequest{
		ChangeType:         "created",
		NotificationUrl:    notificationURL,
		Resource:           "me/mailFolders/Inbox/messages",
		ExpirationDateTime: expiration.Format(time.RFC3339Nano),
		ClientState:        "atlas-outlook-state-secure",
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to marshal subscription req: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", "https://graph.microsoft.com/v1.0/subscriptions", strings.NewReader(string(bodyBytes)))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create subscription req: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to execute subscription request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return "", time.Time{}, fmt.Errorf("microsoft subscription rejected with status %s, body %s", resp.Status, string(respBody))
	}

	var subResp MicrosoftSubscriptionResponse
	if err := json.NewDecoder(resp.Body).Decode(&subResp); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to decode subscription response: %w", err)
	}

	parsedExpiry, err := time.Parse(time.RFC3339, subResp.ExpirationDateTime)
	if err != nil {
		parsedExpiry = expiration
	}

	return subResp.ID, parsedExpiry, nil
}
