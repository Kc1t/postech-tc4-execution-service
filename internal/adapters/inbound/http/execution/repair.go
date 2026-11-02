package executionhandler

import (
	"net/http"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
)

// BeginRepair godoc
// @Summary     Iniciar reparo
// @Description queued_repair → in_repair. Publica execution.RepairStarted.
// @Tags        executions
// @Produce     json
// @Param       os_id path string true "ID da ordem de serviço (UUID)"
// @Param       X-Correlation-ID header string false "Correlação propagada para o evento"
// @Success     200 {object} ExecutionResponse
// @Failure     400 {object} httputil.ErrorResponse "VALIDATION_ERROR (os_id não é UUID ou corpo inválido)"
// @Failure     401 {object} httputil.ErrorResponse "UNAUTHORIZED"
// @Failure     403 {object} httputil.ErrorResponse "FORBIDDEN"
// @Failure     404 {object} httputil.ErrorResponse "EXECUTION_NOT_FOUND"
// @Failure     409 {object} httputil.ErrorResponse "INVALID_TRANSITION / CONCURRENT_UPDATE"
// @Security    BearerAuth
// @Router      /api/v1/executions/{os_id}/repair/start [post]
func (h *Handler) BeginRepair(c *gin.Context) {
	id, ok := osID(c)
	if !ok {
		return
	}
	exec, err := h.beginRepair.Execute(c.Request.Context(), transitionInput(c, id))
	if err != nil {
		httputil.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, toResponse(exec))
}

// FinishRepair godoc
// @Summary     Concluir reparo
// @Description in_repair → finished. Publica execution.RepairFinished.
// @Tags        executions
// @Produce     json
// @Param       os_id path string true "ID da ordem de serviço (UUID)"
// @Param       X-Correlation-ID header string false "Correlação propagada para o evento"
// @Success     200 {object} ExecutionResponse
// @Failure     400 {object} httputil.ErrorResponse "VALIDATION_ERROR (os_id não é UUID ou corpo inválido)"
// @Failure     401 {object} httputil.ErrorResponse "UNAUTHORIZED"
// @Failure     403 {object} httputil.ErrorResponse "FORBIDDEN"
// @Failure     404 {object} httputil.ErrorResponse "EXECUTION_NOT_FOUND"
// @Failure     409 {object} httputil.ErrorResponse "INVALID_TRANSITION / CONCURRENT_UPDATE"
// @Security    BearerAuth
// @Router      /api/v1/executions/{os_id}/repair/finish [post]
func (h *Handler) FinishRepair(c *gin.Context) {
	id, ok := osID(c)
	if !ok {
		return
	}
	exec, err := h.finishRepair.Execute(c.Request.Context(), transitionInput(c, id))
	if err != nil {
		httputil.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, toResponse(exec))
}
