// Package executionhandler expõe a API do mecânico: fila de trabalho, detalhe da execução e
// as transições de diagnóstico e reparo.
package executionhandler

import (
	"strings"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/newrelic"
)

const maxOSIDLength = 64

// Handler agrupa os casos de uso usados pelas rotas.
type Handler struct {
	queue             ports.GetQueueUseCase
	get               ports.GetExecutionUseCase
	beginDiagnosis    ports.BeginDiagnosisUseCase
	completeDiagnosis ports.CompleteDiagnosisUseCase
	beginRepair       ports.BeginRepairUseCase
	finishRepair      ports.FinishRepairUseCase
}

// UseCases são as dependências do handler.
type UseCases struct {
	Queue             ports.GetQueueUseCase
	Get               ports.GetExecutionUseCase
	BeginDiagnosis    ports.BeginDiagnosisUseCase
	CompleteDiagnosis ports.CompleteDiagnosisUseCase
	BeginRepair       ports.BeginRepairUseCase
	FinishRepair      ports.FinishRepairUseCase
}

func NewHandler(uc UseCases) *Handler {
	return &Handler{
		queue:             uc.Queue,
		get:               uc.Get,
		beginDiagnosis:    uc.BeginDiagnosis,
		completeDiagnosis: uc.CompleteDiagnosis,
		beginRepair:       uc.BeginRepair,
		finishRepair:      uc.FinishRepair,
	}
}

// SetupRoutes registra as rotas em rg (o grupo já aplica autenticação e papéis).
func (h *Handler) SetupRoutes(rg *gin.RouterGroup) {
	g := rg.Group("/executions")
	g.GET("/queue", h.Queue)
	g.GET("/:os_id", h.Get)
	g.POST("/:os_id/diagnosis/start", h.BeginDiagnosis)
	g.POST("/:os_id/diagnosis/complete", h.CompleteDiagnosis)
	g.POST("/:os_id/repair/start", h.BeginRepair)
	g.POST("/:os_id/repair/finish", h.FinishRepair)
}

// osID lê e valida o parâmetro de rota; responde 400 e devolve false se inválido.
func osID(c *gin.Context) (string, bool) {
	id := strings.TrimSpace(c.Param("os_id"))
	if id == "" || len(id) > maxOSIDLength {
		httputil.BadRequest(c, "os_id inválido")
		return "", false
	}
	newrelic.FromContext(c.Request.Context()).AddAttribute("os_id", id)
	return id, true
}

// transitionInput monta a entrada com o ator autenticado e o correlation id da requisição.
func transitionInput(c *gin.Context, id string) ports.TransitionInput {
	actorType := entities.ActorMechanic
	if c.GetString(httputil.CtxUserRole) == entities.ActorAdmin {
		actorType = entities.ActorAdmin
	}
	return ports.TransitionInput{
		OSID:          id,
		Actor:         entities.Actor{ID: c.GetString(httputil.CtxUserID), Type: actorType},
		CorrelationID: c.GetString(httputil.CtxCorrelationID),
	}
}
