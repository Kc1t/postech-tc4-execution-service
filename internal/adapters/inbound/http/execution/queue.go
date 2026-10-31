package executionhandler

import (
	"net/http"
	"strconv"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/gin-gonic/gin"
)

// Queue godoc
// @Summary     Fila de trabalho do mecânico
// @Description Sem filtro: execuções em queued_repair (primeiro) e depois queued_diagnosis, mais antigas primeiro em cada raia. Com status: lista só aquela raia (status ativos: queued_diagnosis, in_diagnosis, diagnosed, queued_repair, in_repair).
// @Tags        executions
// @Produce     json
// @Param       status query string false "Filtra por um status ativo" Enums(queued_diagnosis, in_diagnosis, diagnosed, queued_repair, in_repair)
// @Param       limit  query int    false "Máximo de itens (1-200, padrão 50)"
// @Success     200 {object} QueueResponse
// @Failure     400 {object} httputil.ErrorResponse "VALIDATION_ERROR / INVALID_STATUS"
// @Failure     401 {object} httputil.ErrorResponse "UNAUTHORIZED"
// @Failure     403 {object} httputil.ErrorResponse "FORBIDDEN"
// @Security    BearerAuth
// @Router      /executions/queue [get]
func (h *Handler) Queue(c *gin.Context) {
	q := ports.QueueQuery{}
	if s := c.Query("status"); s != "" {
		st, err := entities.ParseStatus(s)
		if err != nil {
			httputil.HandleError(c, err)
			return
		}
		q.Status = st
	}
	if l := c.Query("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			httputil.BadRequest(c, "limit deve ser um inteiro positivo")
			return
		}
		q.Limit = n
	}

	items, err := h.queue.Execute(c.Request.Context(), q)
	if err != nil {
		httputil.HandleError(c, err)
		return
	}
	resp := QueueResponse{Items: make([]ExecutionResponse, 0, len(items)), Count: len(items)}
	for _, e := range items {
		resp.Items = append(resp.Items, toResponse(e))
	}
	c.JSON(http.StatusOK, resp)
}
