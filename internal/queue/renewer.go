package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"atlas-emailagg/internal/config"
	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type SubscriptionRenewer struct {
	dbPool      *pgxpool.Pool
	queueClient *QueueClient
	cfg         *config.Config
}

func NewSubscriptionRenewer(dbPool *pgxpool.Pool, qClient *QueueClient, cfg *config.Config) *SubscriptionRenewer {
	return &SubscriptionRenewer{
		dbPool:      dbPool,
		queueClient: qClient,
		cfg:         cfg,
	}
}

func (sr *SubscriptionRenewer) Start(ctx context.Context) {
	slog.Info("Starting subscription renewal daemon (running every 15 minutes)...")
	ticker := time.NewTicker(15 * time.Minute)
	go func() {
		// Run once on boot
		sr.runRenewalCycle(ctx)

		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				sr.runRenewalCycle(ctx)
			}
		}
	}()
}

func (sr *SubscriptionRenewer) runRenewalCycle(ctx context.Context) {
	slog.Info("Running subscription renewal check...")
	now := time.Now().Unix()
	in24Hours := now + 86400

	// 1. Renew Gmail Watches
	sr.renewGmailWatches(ctx, in24Hours)

	// 2. Renew Outlook Subscriptions
	sr.renewOutlookSubscriptions(ctx, in24Hours)
}

func (sr *SubscriptionRenewer) renewGmailWatches(ctx context.Context, maxScore int64) {
	// Query ZSet for watches expiring in next 24 hours
	keys, err := sr.queueClient.RedisClient.ZRangeByScore(ctx, "zset:gmail_watch_expirations", &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%d", maxScore),
	}).Result()

	if err != nil {
		slog.Error("Failed to fetch Gmail watch expirations from Redis", "error", err)
		return
	}

	for _, emailID := range keys {
		slog.Info("Gmail watch expiring soon, attempting renewal", "email_account_id", emailID)
		acc, err := db.GetEmailAccountByID(ctx, sr.dbPool, emailID)
		if err != nil || acc == nil || acc.Status != "ACTIVE" {
			continue
		}

		if acc.GCPProjectID == nil {
			continue
		}

		clientID, clientSecret, err := db.GetGCPProjectCredentials(ctx, sr.dbPool, *acc.GCPProjectID)
		if err != nil {
			continue
		}

		var creds model.GmailCredentials
		if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
			continue
		}

		// Refresh token
		tokenURL := "https://oauth2.googleapis.com/token"
		formData := url.Values{}
		formData.Set("client_id", clientID)
		formData.Set("client_secret", clientSecret)
		formData.Set("refresh_token", creds.RefreshToken)
		formData.Set("grant_type", "refresh_token")

		resp, err := http.PostForm(tokenURL, formData)
		if err != nil {
			slog.Error("Gmail token refresh in renewer failed", "email", acc.Email, "error", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		var refreshResp struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   int    `json:"expires_in"`
		}
		json.NewDecoder(resp.Body).Decode(&refreshResp)
		resp.Body.Close()

		creds.AccessToken = refreshResp.AccessToken
		creds.Expiry = time.Now().Add(time.Duration(refreshResp.ExpiresIn) * time.Second)

		// Re-trigger watch
		var gcpProjName string
		sr.dbPool.QueryRow(ctx, "SELECT project_name FROM master.gcp_projects WHERE id = $1", *acc.GCPProjectID).Scan(&gcpProjName)

		watchURL := "https://gmail.googleapis.com/gmail/v1/users/me/watch"
		watchPayload := map[string]string{
			"topicName": fmt.Sprintf("projects/%s/topics/%s", gcpProjName, sr.cfg.GCPPubSubTopic),
		}
		watchBytes, _ := json.Marshal(watchPayload)

		watchReq, _ := http.NewRequestWithContext(ctx, "POST", watchURL, strings.NewReader(string(watchBytes)))
		watchReq.Header.Set("Authorization", "Bearer "+refreshResp.AccessToken)
		watchReq.Header.Set("Content-Type", "application/json")

		watchResp, err := http.DefaultClient.Do(watchReq)
		if err == nil && watchResp.StatusCode == http.StatusOK {
			var watch GoogleWatchResponse
			json.NewDecoder(watchResp.Body).Decode(&watch)
			watchResp.Body.Close()

			// Update credentials
			creds.LastHistoryID = watch.HistoryID
			credsBytes, _ := json.Marshal(creds)
			db.UpdateEmailAccountCredentialsAndStatus(ctx, sr.dbPool, acc.ID, string(credsBytes), "ACTIVE")

			var expMs int64
			fmt.Sscanf(watch.Expiration, "%d", &expMs)
			newExpSec := expMs / 1000

			sr.queueClient.RedisClient.ZAdd(ctx, "zset:gmail_watch_expirations", redis.Z{
				Score:  float64(newExpSec),
				Member: acc.ID,
			})
			slog.Info("Successfully renewed Gmail watch", "email", acc.Email, "expiry", time.Unix(newExpSec, 0))
		} else {
			if watchResp != nil {
				watchResp.Body.Close()
			}
			slog.Warn("Failed to execute Gmail watch renewal request", "email", acc.Email)
		}
	}
}

func (sr *SubscriptionRenewer) renewOutlookSubscriptions(ctx context.Context, maxScore int64) {
	// Query ZSet for Outlook subscriptions expiring in next 24 hours
	keys, err := sr.queueClient.RedisClient.ZRangeByScore(ctx, "zset:outlook_subscription_expirations", &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%d", maxScore),
	}).Result()

	if err != nil {
		slog.Error("Failed to fetch Outlook expirations from Redis", "error", err)
		return
	}

	for _, emailID := range keys {
		slog.Info("Outlook subscription expiring soon, attempting renewal", "email_account_id", emailID)
		acc, err := db.GetEmailAccountByID(ctx, sr.dbPool, emailID)
		if err != nil || acc == nil || acc.Status != "ACTIVE" {
			continue
		}

		var creds model.OutlookCredentials
		if err := json.Unmarshal([]byte(acc.Credentials), &creds); err != nil {
			continue
		}

		// 1. Refresh access token
		tokenURL := "https://login.microsoftonline.com/common/oauth2/v2.0/token"
		formData := url.Values{}
		formData.Set("client_id", sr.cfg.OutlookClientID)
		formData.Set("client_secret", sr.cfg.OutlookClientSecret)
		formData.Set("refresh_token", creds.RefreshToken)
		formData.Set("grant_type", "refresh_token")

		resp, err := http.PostForm(tokenURL, formData)
		if err != nil {
			slog.Error("Outlook token refresh in renewer failed", "email", acc.Email, "error", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		var refreshResp OutlookRefreshResponse
		json.NewDecoder(resp.Body).Decode(&refreshResp)
		resp.Body.Close()

		creds.AccessToken = refreshResp.AccessToken
		creds.RefreshToken = refreshResp.RefreshToken
		creds.Expiry = time.Now().Add(time.Duration(refreshResp.ExpiresIn) * time.Second)

		// 2. PATCH Graph Subscription
		notificationURL := os.Getenv("OUTLOOK_NOTIFICATION_URL")
		if notificationURL == "" {
			slog.Warn("OUTLOOK_NOTIFICATION_URL not set in environment, skipping Outlook subscription renewal", "email", acc.Email)
			continue
		}

		newExpiry := time.Now().Add(4000 * time.Minute)
		renewPayload := map[string]string{
			"expirationDateTime": newExpiry.Format(time.RFC3339Nano),
		}
		payloadBytes, _ := json.Marshal(renewPayload)

		patchURL := fmt.Sprintf("https://graph.microsoft.com/v1.0/subscriptions/%s", creds.SubscriptionID)
		req, _ := http.NewRequestWithContext(ctx, "PATCH", patchURL, strings.NewReader(string(payloadBytes)))
		req.Header.Set("Authorization", "Bearer "+refreshResp.AccessToken)
		req.Header.Set("Content-Type", "application/json")

		patchResp, err := http.DefaultClient.Do(req)
		if err == nil && patchResp.StatusCode == http.StatusOK {
			// Update credentials and ZSet
			patchResp.Body.Close()
			credsBytes, _ := json.Marshal(creds)
			db.UpdateEmailAccountCredentialsAndStatus(ctx, sr.dbPool, acc.ID, string(credsBytes), "ACTIVE")

			sr.queueClient.RedisClient.ZAdd(ctx, "zset:outlook_subscription_expirations", redis.Z{
				Score:  float64(newExpiry.Unix()),
				Member: acc.ID,
			})
			slog.Info("Successfully extended Outlook subscription", "email", acc.Email, "expiry", newExpiry)
		} else {
			if patchResp != nil {
				patchResp.Body.Close()
			}
			slog.Warn("Outlook PATCH subscription failed, recreating subscription entirely", "email", acc.Email)

			// Fallback: Re-create subscription from scratch
			subReqURL := "https://graph.microsoft.com/v1.0/subscriptions"
			subPayload := MicrosoftSubscriptionRequest{
				ChangeType:         "created",
				NotificationUrl:    notificationURL,
				Resource:           "me/mailFolders/Inbox/messages",
				ExpirationDateTime: newExpiry.Format(time.RFC3339Nano),
				ClientState:        "atlas-outlook-state-secure",
			}
			subBytes, _ := json.Marshal(subPayload)
			subReq, _ := http.NewRequestWithContext(ctx, "POST", subReqURL, strings.NewReader(string(subBytes)))
			subReq.Header.Set("Authorization", "Bearer "+refreshResp.AccessToken)
			subReq.Header.Set("Content-Type", "application/json")

			subResp, err := http.DefaultClient.Do(subReq)
			if err == nil && subResp.StatusCode == http.StatusCreated {
				var subObj MicrosoftSubscriptionResponse
				json.NewDecoder(subResp.Body).Decode(&subObj)
				subResp.Body.Close()

				creds.SubscriptionID = subObj.ID
				credsBytes, _ := json.Marshal(creds)
				db.UpdateEmailAccountCredentialsAndStatus(ctx, sr.dbPool, acc.ID, string(credsBytes), "ACTIVE")

				parsedExpiry, err := time.Parse(time.RFC3339, subObj.ExpirationDateTime)
				if err != nil {
					parsedExpiry = newExpiry
				}

				sr.queueClient.RedisClient.ZAdd(ctx, "zset:outlook_subscription_expirations", redis.Z{
					Score:  float64(parsedExpiry.Unix()),
					Member: acc.ID,
				})
				slog.Info("Successfully recreated Outlook subscription", "email", acc.Email, "sub_id", subObj.ID)
			} else {
				if subResp != nil {
					subResp.Body.Close()
				}
				slog.Error("Recreating Outlook subscription failed", "email", acc.Email)
			}
		}
	}
}

type GoogleWatchResponse struct {
	HistoryID  string `json:"historyId"`
	Expiration string `json:"expiration"`
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

