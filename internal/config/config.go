package config

import (
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL         string
	RedisURL            string
	Port                string
	OutlookClientID     string
	OutlookClientSecret string
	EnableGmail         bool
	EnableOutlook       bool
	EnableIMAP          bool
	GCPPubSubTopic      string
}

// LoadConfig reads configuration from environment variables with sane defaults.
func LoadConfig() *Config {
	return &Config{
		DatabaseURL:         getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/atlas?sslmode=disable"),
		RedisURL:            getEnv("REDIS_URL", "redis://localhost:6379/0"),
		Port:                getEnv("PORT", "5001"),
		OutlookClientID:     os.Getenv("OUTLOOK_CLIENT_ID"),
		OutlookClientSecret: os.Getenv("OUTLOOK_CLIENT_SECRET"),
		EnableGmail:         getEnvBool("ENABLE_GMAIL", true),
		EnableOutlook:       getEnvBool("ENABLE_OUTLOOK", true),
		EnableIMAP:          getEnvBool("ENABLE_IMAP", true),
		GCPPubSubTopic:      getEnv("GCP_PUBSUB_TOPIC", "gmail-notifications"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return fallback
	}
	return b
}
