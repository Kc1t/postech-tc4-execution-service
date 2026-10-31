// Package healthhandler expõe as probes de liveness e readiness.
package healthhandler

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
)

// Checker verifica uma dependência (ex.: DescribeTable no DynamoDB).
type Checker interface {
	Ping(ctx context.Context) error
}

// CheckerFunc adapta uma função a Checker.
type CheckerFunc func(ctx context.Context) error

func (f CheckerFunc) Ping(ctx context.Context) error { return f(ctx) }

// Response é o corpo das probes com sucesso.
type Response struct {
	Status string            `json:"status" example:"ok"`
	Checks map[string]string `json:"checks,omitempty"`
}

// Handler responde /health/live e /health/ready.
type Handler struct {
	checks  map[string]Checker
	timeout time.Duration
}

func NewHandler(checks map[string]Checker) *Handler {
	return &Handler{checks: checks, timeout: 2 * time.Second}
}

// SetupRoutes registra /health/live e /health/ready.
func (h *Handler) SetupRoutes(r gin.IRouter) {
	r.GET("/health/live", h.Live)
	r.GET("/health/ready", h.Ready)
}

// Live godoc
// @Summary  Liveness
// @Description O processo está de pé (não verifica dependências).
// @Tags     health
// @Produce  json
// @Success  200 {object} Response
// @Router   /health/live [get]
func (h *Handler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, Response{Status: "ok"})
}

// Ready godoc
// @Summary  Readiness
// @Description Verifica as dependências (DynamoDB via DescribeTable). 503 tira o pod do Service.
// @Tags     health
// @Produce  json
// @Success  200 {object} Response
// @Failure  503 {object} httputil.ErrorResponse "SERVICE_UNAVAILABLE"
// @Router   /health/ready [get]
func (h *Handler) Ready(c *gin.Context) {
	names := make([]string, 0, len(h.checks))
	for name := range h.checks {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make(map[string]string, len(names))
	for _, name := range names {
		ctx, cancel := context.WithTimeout(c.Request.Context(), h.timeout)
		err := h.checks[name].Ping(ctx)
		cancel()
		if err != nil {
			httputil.Abort(c, http.StatusServiceUnavailable, httputil.CodeServiceUnavailable, fmt.Sprintf("%s indisponível: %v", name, err))
			return
		}
		result[name] = "ok"
	}
	c.JSON(http.StatusOK, Response{Status: "ready", Checks: result})
}
