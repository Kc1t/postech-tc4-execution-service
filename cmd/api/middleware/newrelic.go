package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/integrations/nrgin"
	"github.com/newrelic/go-agent/v3/newrelic"
)

// NewRelicContext copia a transação que o nrgin guarda no gin.Context para o
// c.Request.Context(). Sem isso as chamadas ao DynamoDB (nrawssdk) e a captura dos headers de
// trace no outbox não enxergam a transação da requisição.
func NewRelicContext() gin.HandlerFunc {
	return func(c *gin.Context) {
		if txn := nrgin.Transaction(c); txn != nil {
			c.Request = c.Request.WithContext(newrelic.NewContext(c.Request.Context(), txn))
		}
		c.Next()
	}
}
