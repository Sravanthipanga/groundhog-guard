// Package config loads runtime configuration from the environment.
package config

import (
	"os"
	"strings"
)

// Config is shared by all groundhog subcommands; each uses the parts it needs.
type Config struct {
	DatabaseURL string   // pgx DSN, e.g. postgres://groundhog:groundhog@localhost:5432/groundhog
	ValkeyAddr  string   // host:port of Valkey/Redis
	Brokers     []string // Kafka bootstrap brokers
	HTTPAddr    string   // listen address for `groundhog api`
	MetricsAddr string   // listen address for the metrics server in relay/consumer
	APIURL      string   // base URL of the API, used by the chaos CLI
}

// FromEnv reads configuration, applying local-development defaults so the
// binary also runs outside Docker against localhost services.
func FromEnv() Config {
	return Config{
		DatabaseURL: getenv("DATABASE_URL", "postgres://groundhog:groundhog@localhost:5432/groundhog?sslmode=disable"),
		ValkeyAddr:  getenv("VALKEY_ADDR", "localhost:6379"),
		Brokers:     splitCSV(getenv("KAFKA_BROKERS", "localhost:9092")),
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
		MetricsAddr: getenv("METRICS_ADDR", ":9091"),
		APIURL:      getenv("API_URL", "http://localhost:8080"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
