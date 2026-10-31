package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/cmd/api/bootstrap"
	"github.com/Kc1t/postech-tc4-execution-service/config"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/observability"
)

// @title           Execution Service API
// @version         1.0
// @description     Execução e Produção da oficina: fila de diagnóstico e reparo, consumida pelo mecânico. Parte da saga orquestrada pelo os-service (Tech Challenge FIAP SOAT — Fase 4).
// @BasePath        /
// @securityDefinitions.apikey BearerAuth
// @in              header
// @name            Authorization
// @description     JWT HS256 com role admin ou mechanic: "Bearer {token}"
func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel).With("service", "execution-service")
	slog.SetDefault(log)

	if err := cfg.Validate(); err != nil {
		log.Error("config_invalid", "error", err)
		os.Exit(1)
	}

	nr, err := observability.NewRelic(cfg.NewRelicAppName, cfg.NewRelicLicenseKey)
	switch {
	case err != nil:
		log.Error("newrelic_disabled", "error", err)
	case nr != nil:
		log.Info("newrelic_enabled", "app_name", cfg.NewRelicAppName)
	default:
		log.Info("newrelic_disabled", "reason", "NEW_RELIC_LICENSE_KEY ausente")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := bootstrap.New(ctx, cfg, log, nr)
	if err != nil {
		log.Error("bootstrap_failed", "error", err)
		os.Exit(1)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", ":"+cfg.AppPort)
	if err != nil {
		log.Error("listen_failed", "error", err)
		os.Exit(1)
	}
	runErr := app.Run(ctx, ln)
	if nr != nil {
		nr.Shutdown(10 * time.Second)
	}
	if runErr != nil {
		log.Error("server_failed", "error", runErr)
		os.Exit(1)
	}
}
