package model

import (
	"time"
)

type GCPProject struct {
	ID          int       `db:"id" json:"id"`
	ProjectName string    `db:"project_name" json:"project_name"`
	ClientID    string    `db:"client_id" json:"client_id"`
	ClientSecret string   `db:"client_secret" json:"client_secret"`
	Domain      string    `db:"domain" json:"domain"`
	ActiveCount int       `db:"active_count" json:"active_count"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

type EmailAccount struct {
	ID             string     `db:"id" json:"id"`
	TenantID       string     `db:"tenant_id" json:"tenant_id"`
	Email          string     `db:"email" json:"email"`
	Provider       string     `db:"provider" json:"provider"` // 'gmail', 'outlook', 'imap'
	Status         string     `db:"status" json:"status"`     // 'ACTIVE', 'ERROR', 'REAUTH_REQUIRED', 'CONNECTION_SUSPENDED'
	GCPProjectID   *int       `db:"gcp_project_id" json:"gcp_project_id"`
	Credentials    string     `db:"credentials" json:"credentials"` // Plaintext JSON credentials string
	LastSyncAt     *time.Time `db:"last_sync_at" json:"last_sync_at"`
	LastError      *string    `db:"last_error" json:"last_error"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at" json:"updated_at"`
}

type GmailCredentials struct {
	ClientID      string    `json:"client_id,omitempty"`
	AccessToken   string    `json:"access_token"`
	RefreshToken  string    `json:"refresh_token"`
	TokenType     string    `json:"token_type"`
	Expiry        time.Time `json:"expiry"`
	LastHistoryID string    `json:"last_history_id,omitempty"`
}

type OutlookCredentials struct {
	AccessToken    string    `json:"access_token"`
	RefreshToken   string    `json:"refresh_token"`
	TokenType      string    `json:"token_type"`
	Expiry         time.Time `json:"expiry"`
	SubscriptionID string    `json:"subscription_id,omitempty"`
}

type IMAPCredentials struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Security string `json:"security"` // 'ssl', 'starttls', 'none'
	Username string `json:"username"`
	Password string `json:"password"`
}

type SubjectRule struct {
	Subject       string `json:"subject"`
	Context       string `json:"context"`
	ExtractMethod string `json:"extract_method"`
}

