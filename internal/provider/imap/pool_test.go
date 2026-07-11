package imap

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"atlas-emailagg/internal/model"
)

func TestIMAPPool_StartAndStop(t *testing.T) {
	// Start Mock Server
	server, err := NewMockIMAPServer()
	if err != nil {
		t.Fatalf("Failed to start mock server: %v", err)
	}
	server.Start()
	defer server.Close()

	host, portStr, _ := net.SplitHostPort(server.Addr())
	port, _ := strconv.Atoi(portStr)

	// Create test credentials
	creds := model.IMAPCredentials{
		Host:     host,
		Port:     port,
		Username: "test@example.com",
		Password: "password",
		Security: "none",
	}
	credsBytes, _ := json.Marshal(creds)

	acc := model.EmailAccount{
		ID:          "acc-1",
		TenantID:    "tenant-1",
		Email:       "test@example.com",
		Provider:    "imap",
		Status:      "ACTIVE",
		Credentials: string(credsBytes),
	}

	// Create Pool
	pool := NewIMAPPool(nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accCtx, accCancel := context.WithCancel(ctx)
	pool.mu.Lock()
	pool.cancelFuncs[acc.ID] = accCancel
	pool.mu.Unlock()

	// Run connectAndIdle directly in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- pool.connectAndIdle(accCtx, acc, creds, server.Addr())
	}()

	// Stop it after a brief moment
	time.Sleep(200 * time.Millisecond)
	accCancel()

	err = <-errChan
	if err != nil && err != context.Canceled {
		t.Errorf("Expected nil or context canceled error, got: %v", err)
	}
}
