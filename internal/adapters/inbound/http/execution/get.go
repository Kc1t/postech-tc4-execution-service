package executionhandler

import (
	"net/http"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
)

// Get godoc
// @Summary     Detalhe da execução
// @Description Estado atual e linha do tempo da execução da OS. Também é consumido pela API composition do os-service.
// @Tags        executions
// @Produce     json
// @Param       os_id path string true "ID da ordem de serviço"
// @Success     200 {object} ExecutionDetailsResponse
// @Failure     400 {object} httputil.ErrorResponse "VALIDATION_ERROR"
// @Failure     401 {object} httputil.ErrorResponse "UNAUTHORIZED"
// @Failure     403 {object} httputil.ErrorResponse "FORBIDDEN"
// @Failure     404 {object} httputil.ErrorResponse "EXECUTION_NOT_FOUND"
// @Security    BearerAuth
// @Router      /api/v1/executions/{os_id} [get]
func (h *Handler) Get(c *gin.Context) {
	id, ok := osID(c)
	if !ok {
		return
	}
	d, err := h.get.Execute(c.Request.Context(), id)
	if err != nil {
		httputil.HandleError(c, err)
		return
	}
	c.JSON(http.StatusOK, toDetailsResponse(d))
}
