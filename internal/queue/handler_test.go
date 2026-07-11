package queue

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestDeduplication(t *testing.T) {
	// Connect to local Redis
	rClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	ctx := context.Background()
	_, err := rClient.Ping(ctx).Result()
	if err != nil {
		t.Skip("Skipping test: Redis not running locally on port 6379")
		return
	}
	defer rClient.Close()

	// Clear test key
	testMsgID := "test-message-12345"
	key := "dedup:email:" + testMsgID
	rClient.Del(ctx, key)

	qClient := &QueueClient{
		RedisClient: rClient,
	}
	handler := &TaskHandler{
		queueClient: qClient,
	}

	// 1. First check: should be false (not duplicate)
	isDup1, err := handler.isDuplicateMessage(ctx, testMsgID)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if isDup1 {
		t.Error("Expected first check to return false (not duplicate), got true")
	}

	// 2. Second check (within 24 hours): should be true (duplicate)
	isDup2, err := handler.isDuplicateMessage(ctx, testMsgID)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !isDup2 {
		t.Error("Expected second check to return true (duplicate), got false")
	}

	// Clean up after test
	rClient.Del(ctx, key)
}
