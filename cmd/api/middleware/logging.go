package middleware

import (
	"log/slog"
	"time"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
	"github.com/gin-gonic/gin"
)

// RequestLogger coloca no contexto da requisição um logger com correlation_id, método, rota e
// trace.id/span.id do New Relic (logs em contexto), e registra uma linha por requisição. Os
// caminhos em skipPaths (probes) não geram a linha de acesso.
func RequestLogger(base *slog.Logger, skipPaths ...string) gin.HandlerFunc {
	skip := make(map[string]bool, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = true
	}
	return func(c *gin.Context) {
		reqLog := logger.WithTrace(c.Request.Context(), base).With(
			"correlation_id", c.GetString(httputil.CtxCorrelationID),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
		)
		c.Request = c.Request.WithContext(logger.ContextWith(c.Request.Context(), reqLog))

		start := time.Now()
		c.Next()
		if skip[c.Request.URL.Path] {
			return
		}

		attrs := []any{
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"route", c.FullPath(),
			"client_ip", c.ClientIP(),
		}
		if uid := c.GetString(httputil.CtxUserID); uid != "" {
			attrs = append(attrs, "user_id", uid, "user_role", c.GetString(httputil.CtxUserRole))
		}
		if len(c.Errors) > 0 {
			attrs = append(attrs, "errors", c.Errors.String())
		}
		switch {
		case c.Writer.Status() >= 500:
			reqLog.Error("request", attrs...)
		case c.Writer.Status() >= 400:
			reqLog.Warn("request", attrs...)
		default:
			reqLog.Info("request", attrs...)
		}
	}
}
