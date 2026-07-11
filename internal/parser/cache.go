package parser

import (
	"context"
	"log/slog"
	"sync"

	"atlas-emailagg/internal/db"
	"atlas-emailagg/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type RuleCache struct {
	dbPool *pgxpool.Pool
	cache  sync.Map // tenantID string -> []model.SubjectRule
}

func NewRuleCache(dbPool *pgxpool.Pool) *RuleCache {
	return &RuleCache{dbPool: dbPool}
}

func (c *RuleCache) GetRules(ctx context.Context, tenantID string) ([]model.SubjectRule, error) {
	if val, ok := c.cache.Load(tenantID); ok {
		return val.([]model.SubjectRule), nil
	}

	rules, err := db.GetTenantSubjectRules(ctx, c.dbPool, tenantID)
	if err != nil {
		return nil, err
	}

	c.cache.Store(tenantID, rules)
	return rules, nil
}

func (c *RuleCache) Invalidate(tenantID string) {
	c.cache.Delete(tenantID)
	slog.Info("Invalidated subject rules cache for tenant", "tenant_id", tenantID)
}

// StartInvalidationListener listens to Redis Pub/Sub for cache invalidations
func StartInvalidationListener(ctx context.Context, rClient *redis.Client, c *RuleCache) {
	pubsub := rClient.Subscribe(ctx, "email_rules:invalidation")
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				c.Invalidate(msg.Payload)
			}
		}
	}()
}
