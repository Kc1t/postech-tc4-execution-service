// Package httputil reúne o formato de erro padronizado da API e as chaves de contexto
// compartilhadas entre middlewares e handlers.
package httputil

import (
	"errors"
	"net/http"

	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/newrelic"
)

// Códigos de erro da API (tabela no README).
const (
	CodeValidation         = "VALIDATION_ERROR"
	CodeUnauthorized       = "UNAUTHORIZED"
	CodeForbidden          = "FORBIDDEN"
	CodeNotFound           = "NOT_FOUND"
	CodeMethodNotAllowed   = "METHOD_NOT_ALLOWED"
	CodeExecutionNotFound  = "EXECUTION_NOT_FOUND"
	CodeInvalidTransition  = "INVALID_TRANSITION"
	CodeConcurrentUpdate   = "CONCURRENT_UPDATE"
	CodeInvalidDiagnosis   = "INVALID_DIAGNOSIS"
	CodeInvalidStatus      = "INVALID_STATUS"
	CodeOriginNotAllowed   = "ORIGIN_NOT_ALLOWED"
	CodeServiceUnavailable = "SERVICE_UNAVAILABLE"
	CodeInternal           = "INTERNAL_ERROR"
)

// Chaves gravadas no gin.Context pelos middlewares.
const (
	CtxCorrelationID = "correlation_id"
	CtxUserID        = "user_id"
	CtxUserRole      = "user_role"
	CtxUserEmail     = "user_email"
	CtxUserName      = "user_name"
)

// ErrorBody é o conteúdo de "error".
type ErrorBody struct {
	Code    string `json:"code" example:"EXECUTION_NOT_FOUND"`
	Message string `json:"message" example:"execução não encontrada: os_id=..."`
}

// ErrorResponse é o corpo de toda resposta de erro: {"error":{"code":"...","message":"..."}}.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

// Abort encerra a requisição com o erro padronizado.
func Abort(c *gin.Context, status int, code, message string) {
	c.AbortWithStatusJSON(status, ErrorResponse{Error: ErrorBody{Code: code, Message: message}})
}

// BadRequest responde 400 VALIDATION_ERROR.
func BadRequest(c *gin.Context, message string) {
	Abort(c, http.StatusBadRequest, CodeValidation, message)
}

// HandleError traduz um erro do caso de uso para a resposta HTTP. Erros não mapeados viram 500
// com mensagem genérica (o detalhe vai só para o log e para o New Relic).
func HandleError(c *gin.Context, err error) {
	status, code := classify(err)
	msg := err.Error()
	if status == http.StatusInternalServerError {
		logger.FromContext(c.Request.Context()).Error("request_failed", "error", err)
		newrelic.FromContext(c.Request.Context()).NoticeError(err)
		msg = "erro interno"
	}
	_ = c.Error(err)
	Abort(c, status, code, msg)
}

func classify(err error) (int, string) {
	switch {
	case errors.Is(err, domainerrors.ErrNotFound):
		return http.StatusNotFound, CodeExecutionNotFound
	case errors.Is(err, domainerrors.ErrConcurrentUpdate):
		return http.StatusConflict, CodeConcurrentUpdate
	case errors.Is(err, domainerrors.ErrInvalidTransition):
		return http.StatusConflict, CodeInvalidTransition
	case errors.Is(err, domainerrors.ErrInvalidDiagnosis):
		return http.StatusUnprocessableEntity, CodeInvalidDiagnosis
	case errors.Is(err, domainerrors.ErrInvalidStatus):
		return http.StatusBadRequest, CodeInvalidStatus
	case errors.Is(err, domainerrors.ErrInvalidExecution):
		return http.StatusBadRequest, CodeValidation
	default:
		return http.StatusInternalServerError, CodeInternal
	}
}
