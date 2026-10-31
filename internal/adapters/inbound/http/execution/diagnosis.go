package executionhandler

import (
	"net/http"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// BeginDiagnosis godoc
// @Summary     Iniciar diagnóstico
// @Description queued_diagnosis → in_diagnosis. Publica execution.DiagnosisStarted.
// @Tags        executions
// @Produce     json
// @Param       os_id path string true "ID da ordem de serviço"
// @Param       X-Correlation-ID header string false "Correlação propagada para o evento"
// @Success     200 {object} ExecutionResponse
// @Failure     400 {object} httputil.ErrorResponse "VALIDATION_ERROR"
// @Failure     401 {object} httputil.ErrorResponse "UNAUTHORIZED"
// @Failure     403 {object} httputil.ErrorResponse "FORBIDDEN"
// @Failure     404 {object} httputil.ErrorResponse "EXECUTION_NOT_FOUND"
// @Failure     409 {object} httputil.ErrorResponse "INVALID_TRANSITION / CONCURRENT_UPDATE"
// @Security    BearerAuth
// @Router      /api/v1/executions/{os_id}/diagnosis/start [post]
func (h *Handler) BeginDiagnosis(c *gin.Context) {
	id, ok := osID(c)
	if !ok {
		return
	}
	exec, err := h.beginDiagnosis.Execute(c.Request.Context(), transitionInput(c, id))
	if err != nil {
		httputil.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, toResponse(exec))
}

// CompleteDiagnosis godoc
// @Summary     Concluir diagnóstico
// @Description in_diagnosis → diagnosed com os serviços e peças necessários. Publica execution.DiagnosisCompleted (a saga reserva as peças e pede o orçamento).
// @Tags        executions
// @Accept      json
// @Produce     json
// @Param       os_id path string true "ID da ordem de serviço"
// @Param       X-Correlation-ID header string false "Correlação propagada para o evento"
// @Param       body body CompleteDiagnosisRequest true "Serviços e peças"
// @Success     200 {object} ExecutionResponse
// @Failure     400 {object} httputil.ErrorResponse "VALIDATION_ERROR"
// @Failure     401 {object} httputil.ErrorResponse "UNAUTHORIZED"
// @Failure     403 {object} httputil.ErrorResponse "FORBIDDEN"
// @Failure     404 {object} httputil.ErrorResponse "EXECUTION_NOT_FOUND"
// @Failure     409 {object} httputil.ErrorResponse "INVALID_TRANSITION / CONCURRENT_UPDATE"
// @Failure     422 {object} httputil.ErrorResponse "INVALID_DIAGNOSIS"
// @Security    BearerAuth
// @Router      /api/v1/executions/{os_id}/diagnosis/complete [post]
func (h *Handler) CompleteDiagnosis(c *gin.Context) {
	id, ok := osID(c)
	if !ok {
		return
	}
	var req CompleteDiagnosisRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		httputil.BadRequest(c, "corpo inválido: "+err.Error())
		return
	}
	exec, err := h.completeDiagnosis.Execute(c.Request.Context(), ports.CompleteDiagnosisInput{
		TransitionInput: transitionInput(c, id),
		Diagnosis:       req.toDomain(),
	})
	if err != nil {
		httputil.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, toResponse(exec))
}
