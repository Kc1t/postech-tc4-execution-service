package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_Padroes(t *testing.T) {
	for _, k := range []string{"APP_PORT", "APP_ENV", "DYNAMODB_TABLE", "MAX_CONCURRENT_REPAIRS", "FAULT_INJECTION_ENABLED",
		"CORS_ALLOWED_ORIGINS", "OUTBOX_RELAY_INTERVAL_MS", "JWT_SECRET", "SQS_EXECUTION_COMMANDS_URL", "SNS_EXECUTION_EVENTS_ARN"} {
		t.Setenv(k, "")
	}
	c := Load()
	if c.AppPort != "8080" || c.AppEnv != EnvDevelopment || c.DynamoDBTable != "executions" || c.MaxConcurrentRepairs != 10 ||
		c.FaultInjectionEnabled || c.OutboxRelayInterval != 500*time.Millisecond || c.NewRelicAppName != "execution-service" ||
		c.JWTSecret != DefaultJWTSecret || len(c.CORSAllowedOrigins) != 1 {
		t.Fatalf("padrões errados: %+v", c)
	}
}

func TestLoad_Variaveis(t *testing.T) {
	t.Setenv("APP_PORT", "9000")
	t.Setenv("DYNAMODB_TABLE", "exec-prod")
	t.Setenv("MAX_CONCURRENT_REPAIRS", "3")
	t.Setenv("FAULT_INJECTION_ENABLED", "true")
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://a.com, https://b.com ,,")
	t.Setenv("OUTBOX_RELAY_INTERVAL_MS", "100")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "5")
	c := Load()
	if c.AppPort != "9000" || c.DynamoDBTable != "exec-prod" || c.MaxConcurrentRepairs != 3 || !c.FaultInjectionEnabled ||
		c.OutboxRelayInterval != 100*time.Millisecond || c.ShutdownTimeout != 5*time.Second {
		t.Fatalf("variáveis não lidas: %+v", c)
	}
	if strings.Join(c.CORSAllowedOrigins, "|") != "https://a.com|https://b.com" {
		t.Fatalf("origens = %v", c.CORSAllowedOrigins)
	}
	t.Setenv("FAULT_INJECTION_ENABLED", "talvez")
	if Load().FaultInjectionEnabled {
		t.Fatal("valor inválido usa o padrão (false)")
	}
}

func valid() *Config {
	return &Config{
		AppEnv: EnvProduction, JWTSecret: "um-segredo-de-verdade", DynamoDBTable: "executions",
		SQSExecutionCommandsURL: "https://sqs/1/execution-commands", SNSExecutionEventsARN: "arn:aws:sns:us-east-1:1:execution-events",
		MaxConcurrentRepairs: 10, CORSAllowedOrigins: []string{"https://app.oficina.com"}, OutboxRelayInterval: time.Second,
	}
}

func TestValidate(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatalf("config válida recusada: %v", err)
	}
	cases := map[string]func(c *Config){
		"sem tabela":         func(c *Config) { c.DynamoDBTable = "" },
		"sem fila":           func(c *Config) { c.SQSExecutionCommandsURL = "" },
		"sem tópico":         func(c *Config) { c.SNSExecutionEventsARN = " " },
		"sem segredo":        func(c *Config) { c.JWTSecret = "" },
		"segredo padrão":     func(c *Config) { c.JWTSecret = DefaultJWTSecret },
		"capacidade zero":    func(c *Config) { c.MaxConcurrentRepairs = 0 },
		"cors curinga":       func(c *Config) { c.CORSAllowedOrigins = []string{"*"} },
		"intervalo inválido": func(c *Config) { c.OutboxRelayInterval = 0 },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			c := valid()
			mut(c)
			if err := c.Validate(); err == nil {
				t.Fatal("esperava erro")
			}
		})
	}
	dev := valid()
	dev.AppEnv = EnvDevelopment
	dev.JWTSecret = DefaultJWTSecret
	if err := dev.Validate(); err != nil {
		t.Fatalf("segredo padrão é aceito em desenvolvimento: %v", err)
	}
}
