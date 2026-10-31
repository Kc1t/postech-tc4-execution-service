// Package config lê a configuração do serviço das variáveis de ambiente (com .env opcional em
// desenvolvimento). As credenciais e o endpoint da AWS seguem a cadeia padrão do SDK
// (AWS_REGION, AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, ...).
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/pkg/env"
	"github.com/joho/godotenv"
)

type AppEnv string

// Mesmos valores do os-service (o pipeline usa APP_ENV=prod); "production" também é aceito.
const (
	EnvDevelopment AppEnv = "development"
	EnvProduction  AppEnv = "prod"
)

func parseAppEnv(v string) AppEnv {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "prod", "production":
		return EnvProduction
	default:
		return EnvDevelopment
	}
}

// DefaultJWTSecret é o segredo de desenvolvimento; recusado em produção.
const DefaultJWTSecret = "change-me-in-production"

type Config struct {
	AppPort  string
	AppEnv   AppEnv
	LogLevel string

	NewRelicAppName    string
	NewRelicLicenseKey string

	JWTSecret          string
	CORSAllowedOrigins []string

	AWSRegion      string
	AWSEndpointURL string

	DynamoDBTable           string
	SQSExecutionCommandsURL string
	SNSExecutionEventsARN   string

	MaxConcurrentRepairs  int
	FaultInjectionEnabled bool

	OutboxRelayInterval time.Duration
	ShutdownTimeout     time.Duration
}

// Load lê a configuração. Não falha: valores ausentes ficam com o padrão e Validate aponta o
// que estiver faltando.
func Load() *Config {
	_ = godotenv.Load()
	return &Config{
		AppPort:                 env.GetOrDefault("APP_PORT", "8080"),
		AppEnv:                  parseAppEnv(env.GetOrDefault("APP_ENV", string(EnvDevelopment))),
		LogLevel:                env.GetOrDefault("LOG_LEVEL", "info"),
		NewRelicAppName:         env.GetOrDefault("NEW_RELIC_APP_NAME", "execution-service"),
		NewRelicLicenseKey:      env.GetOrDefault("NEW_RELIC_LICENSE_KEY", ""),
		JWTSecret:               env.GetOrDefault("JWT_SECRET", DefaultJWTSecret),
		CORSAllowedOrigins:      env.GetListOrDefault("CORS_ALLOWED_ORIGINS", []string{"http://localhost:3000"}),
		AWSRegion:               env.GetOrDefault("AWS_REGION", "us-east-1"),
		AWSEndpointURL:          env.GetOrDefault("AWS_ENDPOINT_URL", ""),
		DynamoDBTable:           env.GetOrDefault("DYNAMODB_TABLE", "executions"),
		SQSExecutionCommandsURL: env.GetOrDefault("SQS_EXECUTION_COMMANDS_URL", ""),
		SNSExecutionEventsARN:   env.GetOrDefault("SNS_EXECUTION_EVENTS_ARN", ""),
		MaxConcurrentRepairs:    env.GetIntOrDefault("MAX_CONCURRENT_REPAIRS", 10),
		FaultInjectionEnabled:   env.GetBoolOrDefault("FAULT_INJECTION_ENABLED", false),
		OutboxRelayInterval:     env.GetDurationOrDefault("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		ShutdownTimeout:         env.GetDurationOrDefault("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
}

// Validate falha cedo (no boot) em configuração que impediria o serviço de funcionar.
func (c *Config) Validate() error {
	var errs []error
	for _, v := range []struct{ name, value string }{
		{"DYNAMODB_TABLE", c.DynamoDBTable},
		{"SQS_EXECUTION_COMMANDS_URL", c.SQSExecutionCommandsURL},
		{"SNS_EXECUTION_EVENTS_ARN", c.SNSExecutionEventsARN},
		{"JWT_SECRET", c.JWTSecret},
	} {
		if strings.TrimSpace(v.value) == "" {
			errs = append(errs, fmt.Errorf("%s obrigatória", v.name))
		}
	}
	if c.AppEnv == EnvProduction && c.JWTSecret == DefaultJWTSecret {
		errs = append(errs, errors.New("JWT_SECRET padrão não é aceito em produção"))
	}
	if c.MaxConcurrentRepairs <= 0 {
		errs = append(errs, errors.New("MAX_CONCURRENT_REPAIRS deve ser positivo"))
	}
	for _, o := range c.CORSAllowedOrigins {
		if o == "*" {
			errs = append(errs, errors.New("CORS_ALLOWED_ORIGINS não aceita '*': liste as origens"))
		}
	}
	if c.OutboxRelayInterval <= 0 {
		errs = append(errs, errors.New("OUTBOX_POLL_INTERVAL deve ser positivo"))
	}
	return errors.Join(errs...)
}
