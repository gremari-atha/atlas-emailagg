package db

import (
	"context"
	"fmt"

	"atlas-emailagg/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetEmailAccountByID retrieves an email account connection by its UUID.
func GetEmailAccountByID(ctx context.Context, pool *pgxpool.Pool, id string) (*model.EmailAccount, error) {
	query := `
		SELECT id, tenant_id, email, provider, status, gcp_project_id, credentials, last_sync_at, last_error, created_at, updated_at
		FROM master.email_accounts
		WHERE id = $1
	`
	row := pool.QueryRow(ctx, query, id)

	var acc model.EmailAccount
	err := row.Scan(
		&acc.ID,
		&acc.TenantID,
		&acc.Email,
		&acc.Provider,
		&acc.Status,
		&acc.GCPProjectID,
		&acc.Credentials,
		&acc.LastSyncAt,
		&acc.LastError,
		&acc.CreatedAt,
		&acc.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan email account: %w", err)
	}

	return &acc, nil
}

// UpdateEmailAccountCredentialsAndStatus updates the credentials JSON and status for an account.
func UpdateEmailAccountCredentialsAndStatus(ctx context.Context, pool *pgxpool.Pool, id string, credentials string, status string) error {
	query := `
		UPDATE master.email_accounts
		SET credentials = $1, status = $2, last_error = NULL, updated_at = NOW()
		WHERE id = $3
	`
	_, err := pool.Exec(ctx, query, credentials, status, id)
	if err != nil {
		return fmt.Errorf("failed to update email account credentials: %w", err)
	}
	return nil
}

// UpdateEmailAccountStatus updates the status and last error fields of an account.
func UpdateEmailAccountStatus(ctx context.Context, pool *pgxpool.Pool, id string, status string, lastErr string) error {
	var query string
	var err error
	if lastErr != "" {
		query = `
			UPDATE master.email_accounts
			SET status = $1, last_error = $2, updated_at = NOW()
			WHERE id = $3
		`
		_, err = pool.Exec(ctx, query, status, lastErr, id)
	} else {
		query = `
			UPDATE master.email_accounts
			SET status = $1, last_error = NULL, updated_at = NOW()
			WHERE id = $2
		`
		_, err = pool.Exec(ctx, query, status, id)
	}

	if err != nil {
		return fmt.Errorf("failed to update email account status: %w", err)
	}
	return nil
}

// GetEmailAccountByEmailAndProvider fetches an account by email and provider name.
func GetEmailAccountByEmailAndProvider(ctx context.Context, pool *pgxpool.Pool, email string, provider string) (*model.EmailAccount, error) {
	query := `
		SELECT id, tenant_id, email, provider, status, gcp_project_id, credentials, last_sync_at, last_error, created_at, updated_at
		FROM master.email_accounts
		WHERE email = $1 AND provider = $2 AND status = 'ACTIVE'
	`
	row := pool.QueryRow(ctx, query, email, provider)

	var acc model.EmailAccount
	err := row.Scan(
		&acc.ID,
		&acc.TenantID,
		&acc.Email,
		&acc.Provider,
		&acc.Status,
		&acc.GCPProjectID,
		&acc.Credentials,
		&acc.LastSyncAt,
		&acc.LastError,
		&acc.CreatedAt,
		&acc.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch email account: %w", err)
	}

	return &acc, nil
}

// GetTenantSubjectRules retrieves the active matching subjects from the tenant schema.
func GetTenantSubjectRules(ctx context.Context, pool *pgxpool.Pool, tenantID string) ([]model.SubjectRule, error) {
	// Query the schema dynamically using format (TimescaleDB / Postgres requires schema quotes)
	query := fmt.Sprintf(`
		SELECT subject, context, extract_method
		FROM "%s".email_subject
	`, tenantID)

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant subject rules: %w", err)
	}
	defer rows.Close()

	var rules []model.SubjectRule
	for rows.Next() {
		var rule model.SubjectRule
		if err := rows.Scan(&rule.Subject, &rule.Context, &rule.ExtractMethod); err != nil {
			return nil, fmt.Errorf("failed to scan subject rule: %w", err)
		}
		rules = append(rules, rule)
	}

	return rules, nil
}

// GetGCPProjectCredentials fetches the client ID and secret for a GCP project.
func GetGCPProjectCredentials(ctx context.Context, pool *pgxpool.Pool, id int) (string, string, error) {
	query := `
		SELECT client_id, client_secret
		FROM master.gcp_projects
		WHERE id = $1
	`
	var clientID, clientSecret string
	err := pool.QueryRow(ctx, query, id).Scan(&clientID, &clientSecret)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch GCP project credentials: %w", err)
	}
	return clientID, clientSecret, nil
}

// GetActiveIMAPAccounts retrieves all active IMAP email accounts.
func GetActiveIMAPAccounts(ctx context.Context, pool *pgxpool.Pool) ([]model.EmailAccount, error) {
	query := `
		SELECT id, tenant_id, email, provider, status, gcp_project_id, credentials, last_sync_at, last_error, created_at, updated_at
		FROM master.email_accounts
		WHERE provider = 'imap' AND status = 'ACTIVE'
	`
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active imap accounts: %w", err)
	}
	defer rows.Close()

	var accounts []model.EmailAccount
	for rows.Next() {
		var acc model.EmailAccount
		err := rows.Scan(
			&acc.ID,
			&acc.TenantID,
			&acc.Email,
			&acc.Provider,
			&acc.Status,
			&acc.GCPProjectID,
			&acc.Credentials,
			&acc.LastSyncAt,
			&acc.LastError,
			&acc.CreatedAt,
			&acc.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan imap account: %w", err)
		}
		accounts = append(accounts, acc)
	}
	return accounts, nil
}

// GetEmailAccountBySubscriptionID retrieves an Outlook email account by its webhook Subscription ID.
func GetEmailAccountBySubscriptionID(ctx context.Context, pool *pgxpool.Pool, subID string) (*model.EmailAccount, error) {
	query := `
		SELECT id, tenant_id, email, provider, status, gcp_project_id, credentials, last_sync_at, last_error, created_at, updated_at
		FROM master.email_accounts
		WHERE provider = 'outlook' AND credentials LIKE '%' || $1 || '%'
	`
	row := pool.QueryRow(ctx, query, subID)

	var acc model.EmailAccount
	err := row.Scan(
		&acc.ID,
		&acc.TenantID,
		&acc.Email,
		&acc.Provider,
		&acc.Status,
		&acc.GCPProjectID,
		&acc.Credentials,
		&acc.LastSyncAt,
		&acc.LastError,
		&acc.CreatedAt,
		&acc.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan email account by subscription ID: %w", err)
	}
	return &acc, nil
}



