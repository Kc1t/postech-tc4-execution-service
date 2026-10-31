// Package middleware contém os middlewares HTTP do serviço: correlação, log de requisição,
// contexto do New Relic, CORS com allowlist e autenticação JWT.
package middleware

import (
	"crypto/rand"
	"encoding/hex"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/newrelic"
)

const (
	// CorrelationHeader é o header de correlação aceito e devolvido.
	CorrelationHeader = "X-Correlation-ID"
	requestIDHeader   = "X-Request-ID"
	maxCorrelationLen = 128
)

// CorrelationID aceita o X-Correlation-ID do cliente (ou X-Request-ID), gera um novo quando
// ausente ou inválido, devolve no header da resposta e grava no contexto: dali ele vai para os
// logs e para o correlation_id dos eventos publicados.
func CorrelationID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(CorrelationHeader)
		if id == "" {
			id = c.GetHeader(requestIDHeader)
		}
		if !validCorrelationID(id) {
			id = newCorrelationID()
		}
		c.Set(httputil.CtxCorrelationID, id)
		c.Writer.Header().Set(CorrelationHeader, id)
		newrelic.FromContext(c.Request.Context()).AddAttribute("correlation_id", id)
		c.Next()
	}
}

// validCorrelationID aceita só caracteres seguros, para o valor não injetar nada nos logs.
func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationLen {
		return false
	}
	for _, r := range id {
		ok := r == '-' || r == '_' || r == '.' || r == ':' ||
			(r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		if !ok {
			return false
		}
	}
	return true
}

func newCorrelationID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
