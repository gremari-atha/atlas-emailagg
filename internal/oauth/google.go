package oauth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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

// SetupGoogleRoutes registers OAuth endpoints for Google/Gmail.
func SetupGoogleRoutes(r chi.Router, dbPool *pgxpool.Pool, qClient *queue.QueueClient, cfg *config.Config) {
	handler := &GoogleHandler{
		dbPool:      dbPool,
		queueClient: qClient,
		cfg:         cfg,
	}
	// Default Google routes
	r.Get("/google/connect", handler.HandleConnect)
	r.Get("/google/callback", handler.HandleCallback)
	r.Post("/google/revoke", handler.HandleRevoke)

	// Gmail alias routes matching dashboard provider name
	r.Get("/gmail/connect", handler.HandleConnect)
	r.Get("/gmail/callback", handler.HandleCallback)
	r.Post("/gmail/revoke", handler.HandleRevoke)
}

type GoogleHandler struct {
	dbPool      *pgxpool.Pool
	queueClient *queue.QueueClient
	cfg         *config.Config
}

type GoogleOAuthState struct {
	EmailID      string `json:"email_id"`
	GCPProjectID int    `json:"gcp_project_id"`
	CSRF         string `json:"csrf"`
}

func (h *GoogleHandler) getRedirectURI(r *http.Request, domain string) string {
	// Entra/Google OAuth redirect needs HTTPS. Local testing can fallback
	scheme := "https"
	if strings.Contains(r.Host, "localhost") || strings.Contains(r.Host, "127.0.0.1") {
		scheme = "http"
		return fmt.Sprintf("%s://%s/oauth/google/callback", scheme, r.Host)
	}
	return fmt.Sprintf("%s://%s/oauth/google/callback", scheme, domain)
}

func (h *GoogleHandler) HandleConnect(w http.ResponseWriter, r *http.Request) {
	emailID := r.URL.Query().Get("email_id")
	if emailID == "" {
		http.Error(w, "Missing email_id query parameter", http.StatusBadRequest)
		return
	}

	// 1. Verify email account
	acc, err := db.GetEmailAccountByID(r.Context(), h.dbPool, emailID)
	if err != nil {
		slog.Error("Database query failed in Gmail Connect", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if acc == nil {
		http.Error(w, "Email account not registered", http.StatusNotFound)
		return
	}

	// 2. Select available GCP project from pool
	var gcpProj model.GCPProject
	query := `
		SELECT id, project_name, client_id, client_secret, domain, active_count, created_at, updated_at
		FROM master.gcp_projects
		WHERE active_count < 90
		ORDER BY active_count ASC, id ASC
		LIMIT 1
	`
	err = h.dbPool.QueryRow(r.Context(), query).Scan(
		&gcpProj.ID,
		&gcpProj.ProjectName,
		&gcpProj.ClientID,
		&gcpProj.ClientSecret,
		&gcpProj.Domain,
		&gcpProj.ActiveCount,
		&gcpProj.CreatedAt,
		&gcpProj.UpdatedAt,
	)
	if err != nil {
		slog.Error("Failed to find available GCP project in pool", "error", err)
		http.Error(w, "No available GCP integration application slots. Contact administrator.", http.StatusServiceUnavailable)
		return
	}

	// 3. Construct OAuth state
	stateObj := GoogleOAuthState{
		EmailID:      emailID,
		GCPProjectID: gcpProj.ID,
		CSRF:         "secure-gmail-csrf-token",
	}
	stateBytes, _ := json.Marshal(stateObj)
	state := base64.URLEncoding.EncodeToString(stateBytes)

	redirectURI := h.getRedirectURI(r, gcpProj.Domain)

	authURL := fmt.Sprintf(
		"https://accounts.google.com/o/oauth2/v2/auth?client_id=%s&redirect_uri=%s&response_type=code&scope=%s&access_type=offline&prompt=consent&state=%s",
		url.QueryEscape(gcpProj.ClientID),
		url.QueryEscape(redirectURI),
		url.QueryEscape("https://www.googleapis.com/auth/gmail.readonly"),
		url.QueryEscape(state),
	)

	http.Redirect(w, r, authURL, http.StatusTemporaryRedirect)
}

type GoogleTokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
}

type GoogleProfileResponse struct {
	EmailAddress string `json:"emailAddress"`
}

type GoogleWatchResponse struct {
	HistoryID  string `json:"historyId"`
	Expiration string `json:"expiration"` // Unix time in milliseconds
}

func (h *GoogleHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	stateEncoded := r.URL.Query().Get("state")

	if code == "" || stateEncoded == "" {
		http.Error(w, "Missing code or state parameters", http.StatusBadRequest)
		return
	}

	// 1. Decode state
	stateBytes, err := base64.URLEncoding.DecodeString(stateEncoded)
	if err != nil {
		http.Error(w, "Invalid state format", http.StatusBadRequest)
		return
	}

	var state GoogleOAuthState
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		http.Error(w, "Failed to parse state", http.StatusBadRequest)
		return
	}

	// 2. Fetch email account and GCP project details
	acc, err := db.GetEmailAccountByID(r.Context(), h.dbPool, state.EmailID)
	if err != nil || acc == nil {
		http.Error(w, "Email connection record not found", http.StatusNotFound)
		return
	}

	var gcpProj model.GCPProject
	gcpQuery := `
		SELECT id, project_name, client_id, client_secret, domain, active_count
		FROM master.gcp_projects
		WHERE id = $1
	`
	err = h.dbPool.QueryRow(r.Context(), gcpQuery, state.GCPProjectID).Scan(
		&gcpProj.ID,
		&gcpProj.ProjectName,
		&gcpProj.ClientID,
		&gcpProj.ClientSecret,
		&gcpProj.Domain,
		&gcpProj.ActiveCount,
	)
	if err != nil {
		http.Error(w, "Associated GCP project not found", http.StatusInternalServerError)
		return
	}

	// 3. Exchange OAuth code for tokens
	redirectURI := h.getRedirectURI(r, gcpProj.Domain)
	tokenURL := "https://oauth2.googleapis.com/token"
	formData := url.Values{}
	formData.Set("client_id", gcpProj.ClientID)
	formData.Set("client_secret", gcpProj.ClientSecret)
	formData.Set("code", code)
	formData.Set("redirect_uri", redirectURI)
	formData.Set("grant_type", "authorization_code")

	resp, err := http.PostForm(tokenURL, formData)
	if err != nil {
		http.Error(w, "Token exchange connection failed", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		slog.Error("Google token exchange rejected", "status", resp.Status, "body", string(bodyBytes))
		http.Error(w, "Google token exchange failed", http.StatusBadGateway)
		return
	}

	var tokenResp GoogleTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		http.Error(w, "Failed to parse token response", http.StatusInternalServerError)
		return
	}

	// 4. Verify authenticated Google profile email matches
	profileReq, _ := http.NewRequestWithContext(r.Context(), "GET", "https://gmail.googleapis.com/gmail/v1/users/me/profile", nil)
	profileReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenResp.AccessToken))

	profileResp, err := http.DefaultClient.Do(profileReq)
	if err != nil || profileResp.StatusCode != http.StatusOK {
		http.Error(w, "Failed to verify Gmail profile", http.StatusBadGateway)
		return
	}
	defer profileResp.Body.Close()

	var profile GoogleProfileResponse
	json.NewDecoder(profileResp.Body).Decode(&profile)

	if strings.ToLower(strings.TrimSpace(profile.EmailAddress)) != strings.ToLower(strings.TrimSpace(acc.Email)) {
		http.Error(w, fmt.Sprintf("Email mismatch: Authenticated with '%s' but expected '%s'", profile.EmailAddress, acc.Email), http.StatusForbidden)
		return
	}

	// 5. Register Gmail Push Notification (users.watch)
	watchURL := "https://gmail.googleapis.com/gmail/v1/users/me/watch"
	watchPayload := map[string]string{
		"topicName": fmt.Sprintf("projects/%s/topics/gmail-notifications", gcpProj.ProjectName),
	}
	watchBytes, _ := json.Marshal(watchPayload)

	watchReq, _ := http.NewRequestWithContext(r.Context(), "POST", watchURL, strings.NewReader(string(watchBytes)))
	watchReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenResp.AccessToken))
	watchReq.Header.Set("Content-Type", "application/json")

	watchResp, err := http.DefaultClient.Do(watchReq)
	var watch GoogleWatchResponse
	if err == nil && watchResp.StatusCode == http.StatusOK {
		json.NewDecoder(watchResp.Body).Decode(&watch)
		watchResp.Body.Close()
	} else if watchResp != nil {
		watchResp.Body.Close()
	}

	// Determine starting history ID
	historyID := "0"
	if watch.HistoryID != "" {
		historyID = watch.HistoryID
	}

	// 6. Save credentials inside master.email_accounts
	creds := map[string]interface{}{
		"client_id":       gcpProj.ClientID,
		"access_token":    tokenResp.AccessToken,
		"refresh_token":   tokenResp.RefreshToken,
		"token_type":      tokenResp.TokenType,
		"expiry":          time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second),
		"last_history_id": historyID,
	}
	credsBytes, _ := json.Marshal(creds)

	// Save to DB and update project capacity
	tx, err := h.dbPool.Begin(r.Context())
	if err != nil {
		http.Error(w, "Database transaction failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	_, err = tx.Exec(r.Context(), `
		UPDATE master.email_accounts
		SET credentials = $1, status = 'ACTIVE', gcp_project_id = $2, provider = 'gmail', last_error = NULL, updated_at = NOW()
		WHERE id = $3
	`, string(credsBytes), gcpProj.ID, acc.ID)
	if err != nil {
		http.Error(w, "Failed to save email account credentials", http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(r.Context(), `
		UPDATE master.gcp_projects
		SET active_count = active_count + 1, updated_at = NOW()
		WHERE id = $1
	`, gcpProj.ID)
	if err != nil {
		http.Error(w, "Failed to increment project capacity", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		http.Error(w, "Failed to commit database transaction", http.StatusInternalServerError)
		return
	}

	// Store watch expiration in Redis ZSet (convert milliseconds to Unix seconds)
	if watch.Expiration != "" {
		var expMs int64
		fmt.Sscanf(watch.Expiration, "%d", &expMs)
		expSec := expMs / 1000
		h.queueClient.RedisClient.ZAdd(r.Context(), "zset:gmail_watch_expirations", redis.Z{
			Score:  float64(expSec),
			Member: acc.ID,
		})
	}

	// 7. Publish WebSocket broadcast event
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

	slog.Info("Successfully linked Gmail account", "email", acc.Email, "project", gcpProj.ProjectName)

	// 8. Return success page
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

func (h *GoogleHandler) HandleRevoke(w http.ResponseWriter, r *http.Request) {
	emailID := r.URL.Query().Get("email_id")
	if emailID == "" {
		http.Error(w, "Missing email_id", http.StatusBadRequest)
		return
	}

	acc, err := db.GetEmailAccountByID(r.Context(), h.dbPool, emailID)
	if err != nil || acc == nil {
		http.Error(w, "Email connection not found", http.StatusNotFound)
		return
	}

	var creds map[string]interface{}
	json.Unmarshal([]byte(acc.Credentials), &creds)

	refToken, _ := creds["refresh_token"].(string)
	if refToken != "" {
		// Call Google Revoke API
		revokeURL := fmt.Sprintf("https://oauth2.googleapis.com/revoke?token=%s", url.QueryEscape(refToken))
		resp, err := http.Post(revokeURL, "application/x-www-form-urlencoded", nil)
		if err == nil {
			resp.Body.Close()
			slog.Info("Successfully revoked Google OAuth token programmatically", "email", acc.Email)
		}
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "revoked"}`))
}
