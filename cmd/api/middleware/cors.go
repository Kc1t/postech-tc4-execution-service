package middleware

import (
	"net/http"
	"strings"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
)

// CORS libera só as origens da allowlist (CORS_ALLOWED_ORIGINS), nunca "*". Requisição de
// origem não listada segue sem os headers (o navegador bloqueia); preflight de origem não
// listada recebe 403.
func CORS(allowed []string) gin.HandlerFunc {
	set := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		set[strings.TrimRight(o, "/")] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin == "" {
			c.Next()
			return
		}
		c.Writer.Header().Add("Vary", "Origin")
		ok := set[strings.TrimRight(origin, "/")]
		if ok {
			h := c.Writer.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, "+CorrelationHeader)
			h.Set("Access-Control-Expose-Headers", CorrelationHeader)
			h.Set("Access-Control-Max-Age", "600")
		}
		if c.Request.Method == http.MethodOptions {
			if !ok {
				httputil.Abort(c, http.StatusForbidden, httputil.CodeOriginNotAllowed, "origem não permitida")
				return
			}
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
