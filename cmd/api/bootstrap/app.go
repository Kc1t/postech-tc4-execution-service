// Package bootstrap faz a injeção de dependências (adapters → casos de uso → handlers) e
// controla o ciclo de vida do processo: servidor HTTP, consumidor da fila e relay do outbox.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/cmd/api/routes"
	"github.com/Kc1t/postech-tc4-execution-service/config"
	executionhandler "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http/execution"
	healthhandler "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http/health"
	"github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/messaging/commands"
	dynamorepo "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/outbound/dynamodb"
	executionuc "github.com/Kc1t/postech-tc4-execution-service/internal/application/usecase/execution"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/awsx"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/newrelic"
)

// App é o serviço montado.
type App struct {
	cfg      *config.Config
	log      *slog.Logger
	Router   *gin.Engine
	Consumer *messaging.Consumer
	Relay    *dynamorepo.Relay
	Repo     *dynamorepo.Repository
}

// New monta o serviço a partir da configuração. Os clientes AWS usam a cadeia padrão do SDK
// (AWS_ENDPOINT_URL aponta para o LocalStack em desenvolvimento) com o middleware do New Relic.
func New(ctx context.Context, cfg *config.Config, log *slog.Logger, nr *newrelic.Application) (*App, error) {
	if cfg.AppEnv == config.EnvProduction {
		gin.SetMode(gin.ReleaseMode)
	}
	awsCfg, err := awsx.LoadConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: configuração AWS: %w", err)
	}
	ddbClient := dynamodb.NewFromConfig(awsCfg)
	snsClient := sns.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)

	repo := dynamorepo.NewRepository(ddbClient, cfg.DynamoDBTable)
	deps := executionuc.Deps{
		Repo:   repo,
		Events: messaging.Destination{Kind: messaging.Topic, Name: cfg.SNSExecutionEventsARN},
	}

	execHandler := executionhandler.NewHandler(executionhandler.UseCases{
		Queue:             executionuc.NewGetQueue(repo),
		Get:               executionuc.NewGetExecution(repo),
		BeginDiagnosis:    executionuc.NewBeginDiagnosis(deps),
		CompleteDiagnosis: executionuc.NewCompleteDiagnosis(deps),
		BeginRepair:       executionuc.NewBeginRepair(deps),
		FinishRepair:      executionuc.NewFinishRepair(deps),
	})
	health := healthhandler.NewHandler(map[string]healthhandler.Checker{"dynamodb": repo})

	consumer := messaging.NewConsumer(sqsClient, cfg.SQSExecutionCommandsURL, executionuc.Consumer, nr, log)
	commands.NewHandler(
		executionuc.NewStartDiagnosis(deps),
		executionuc.NewStartRepair(deps, executionuc.StartRepairConfig{
			MaxConcurrentRepairs:  cfg.MaxConcurrentRepairs,
			FaultInjectionEnabled: cfg.FaultInjectionEnabled,
		}),
		executionuc.NewCancelExecution(deps),
	).Register(consumer)

	relay := dynamorepo.NewRelay(ddbClient, cfg.DynamoDBTable, messaging.NewAWSPublisher(snsClient, sqsClient), nr, log).
		WithInterval(cfg.OutboxRelayInterval)

	router := routes.New(routes.Deps{
		Log:         log,
		NewRelic:    nr,
		JWTSecret:   cfg.JWTSecret,
		CORSOrigins: cfg.CORSAllowedOrigins,
		Executions:  execHandler,
		Health:      health,
	})

	if cfg.FaultInjectionEnabled {
		log.Warn("fault_injection_enabled", "detail", "simulate_failure=repair recusa o reparo")
	}
	return &App{cfg: cfg, log: log, Router: router, Consumer: consumer, Relay: relay, Repo: repo}, nil
}

// Run atende HTTP em ln e roda consumidor e relay até ctx ser cancelado (SIGTERM).
//
// Desligamento: para de aceitar requisições e espera as em andamento (http.Server.Shutdown),
// depois para o consumidor e o relay e faz uma última varredura do outbox para publicar os
// eventos das últimas requisições. Mensagens interrompidas no meio voltam para a fila depois do
// visibility timeout e são reprocessadas sem efeito duplicado (inbox).
func (a *App) Run(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           a.Router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	workers, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	var wg sync.WaitGroup
	wg.Go(func() { a.Consumer.Run(workers) })
	wg.Go(func() { a.Relay.Run(workers) })

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	a.log.Info("server_listening", "addr", ln.Addr().String(), "env", string(a.cfg.AppEnv))

	var runErr error
	select {
	case <-ctx.Done():
		a.log.Info("shutdown_started")
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("servidor HTTP: %w", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		a.log.Error("http_shutdown_failed", "error", err)
	}
	stopWorkers()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		a.log.Warn("shutdown_timeout", "detail", "consumidor ou relay não pararam a tempo")
	}
	if n, err := a.Relay.Flush(shutdownCtx); err != nil {
		a.log.Warn("outbox_final_flush_failed", "error", err)
	} else if n > 0 {
		a.log.Info("outbox_final_flush", "published", n)
	}
	a.log.Info("shutdown_completed")
	return runErr
}
