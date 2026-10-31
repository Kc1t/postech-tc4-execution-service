// Package routes monta o router HTTP: middlewares na ordem certa, probes, Swagger e a API.
package routes

import (
	"log/slog"
	"net/http"

	"github.com/Kc1t/postech-tc4-execution-service/cmd/api/middleware"
	_ "github.com/Kc1t/postech-tc4-execution-service/docs" // documentação Swagger gerada pelo swag
	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	executionhandler "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http/execution"
	healthhandler "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http/health"
	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/integrations/nrgin"
	"github.com/newrelic/go-agent/v3/newrelic"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// Deps são as dependências do router.
type Deps struct {
	Log         *slog.Logger
	NewRelic    *newrelic.Application // nil = agente desligado
	JWTSecret   string
	CORSOrigins []string
	Executions  *executionhandler.Handler
	Health      *healthhandler.Handler
}

// New devolve o router configurado.
//
// Ordem dos middlewares: recuperação de pânico → transação New Relic (nrgin) → transação no
// contexto da requisição → correlation id → logger da requisição (já com trace.id) → CORS.
func New(d Deps) *gin.Engine {
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.Use(gin.CustomRecovery(func(c *gin.Context, rec any) {
		d.Log.Error("panic_recovered", "panic", rec, "path", c.Request.URL.Path)
		httputil.Abort(c, http.StatusInternalServerError, httputil.CodeInternal, "erro interno")
	}))
	if d.NewRelic != nil {
		r.Use(nrgin.Middleware(d.NewRelic))
	}
	r.Use(
		middleware.NewRelicContext(),
		middleware.CorrelationID(),
		middleware.RequestLogger(d.Log, "/health/live", "/health/ready"),
		middleware.CORS(d.CORSOrigins),
	)

	r.NoRoute(func(c *gin.Context) {
		httputil.Abort(c, http.StatusNotFound, httputil.CodeNotFound, "rota não encontrada")
	})
	r.NoMethod(func(c *gin.Context) {
		httputil.Abort(c, http.StatusMethodNotAllowed, httputil.CodeNotFound, "método não permitido nesta rota")
	})

	d.Health.SetupRoutes(r)
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	api := r.Group("/api/v1")
	api.Use(middleware.Auth(d.JWTSecret), middleware.RequireRole(entities.ActorAdmin, entities.ActorMechanic))
	d.Executions.SetupRoutes(api)
	return r
}
