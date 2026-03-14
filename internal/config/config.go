package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"

	"transactional-outbox-go/internal/logger"

	"go.uber.org/zap"
)

type AppConfig struct {
	Port           string `mapstructure:"port"`
	PollerInterval int    `mapstructure:"poller_interval"`
	BatchSize      int    `mapstructure:"batch_size"`
	MaxRetries     int    `mapstructure:"max_retries"`
	BaseBackoff    int    `mapstructure:"base_backoff"`
	BreakerBackoff int    `mapstructure:"breaker_backoff"`

	CleanupInterval      int `mapstructure:"cleanup_interval"`
	CleanupSentThreshold int `mapstructure:"cleanup_sent_threshold"`
	CleanupDeadThreshold int `mapstructure:"cleanup_dead_threshold"`

	// Circuit Breaker settings
	CBMaxRequests        int `mapstructure:"cb_max_requests"`         // Max requests in half-open state
	CBInterval           int `mapstructure:"cb_interval"`             // Interval in seconds to clear counts
	CBTimeout            int `mapstructure:"cb_timeout"`              // Timeout in seconds before half-open
	CBConsecutiveFailure int `mapstructure:"cb_consecutive_failures"` // Failures before opening

	// Worker pool settings
	WorkerCount int `mapstructure:"worker_count"` // Number of concurrent workers

	// Rate limiting
	RateLimitPerSecond int `mapstructure:"rate_limit_per_second"` // Max events per second (0 = unlimited)
}

type DBConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
	SSLMode  string `mapstructure:"sslmode"`
}

type KafkaConfig struct {
	Brokers       []string `mapstructure:"brokers"`
	Topic         string   `mapstructure:"topic"`
	DLQTopic      string   `mapstructure:"dlq_topic"`
	Acks          string   `mapstructure:"acks"`
	Retries       int      `mapstructure:"retries"`
	BatchSize     int      `mapstructure:"batch_size"`
	LingerMs      int      `mapstructure:"linger_ms"`
	Compression   string   `mapstructure:"compression"`
	SecurityProto string   `mapstructure:"security_protocol"`
	SASLMechanism string   `mapstructure:"sasl_mechanism"`
	SASLUsername  string   `mapstructure:"sasl_username"`
	SASLPassword  string   `mapstructure:"sasl_password"`
}

type Config struct {
	App   AppConfig   `mapstructure:"app"`
	DB    DBConfig    `mapstructure:"db"`
	Kafka KafkaConfig `mapstructure:"kafka"`
}

func LoadConfig(path string) (*Config, error) {
	wd, _ := os.Getwd()
	logger.L().Debug("Current working dir", zap.String("wd", wd))

	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(path)

	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		logger.L().Error("Error in reading the config file", zap.Error(err))
		return nil, fmt.Errorf("error in reading the config file: %w", err)
	}

	var cfg Config

	if err := v.Unmarshal(&cfg); err != nil {
		logger.L().Error("Error in loading Config", zap.Error(err))
		return nil, fmt.Errorf("error in loading Config: %w", err)
	}

	return &cfg, nil

}
