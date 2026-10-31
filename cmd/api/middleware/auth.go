package middleware

import (
	"net/http"
	"strings"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// Auth valida o JWT HS256 (JWT_SECRET compartilhado entre os serviços) e grava sub, role,
// email e name no contexto. Só HS256 é aceito: o algoritmo do token não escolhe a verificação.
func Auth(secret string) gin.HandlerFunc {
	parser := jwt.NewParser(jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	key := []byte(secret)
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		scheme, raw, found := strings.Cut(header, " ")
		if !found || !strings.EqualFold(scheme, "bearer") || strings.TrimSpace(raw) == "" {
			httputil.Abort(c, http.StatusUnauthorized, httputil.CodeUnauthorized, "token Bearer ausente ou mal formatado")
			return
		}
		claims := jwt.MapClaims{}
		if _, err := parser.ParseWithClaims(strings.TrimSpace(raw), claims, func(*jwt.Token) (any, error) { return key, nil }); err != nil {
			httputil.Abort(c, http.StatusUnauthorized, httputil.CodeUnauthorized, "token inválido ou expirado")
			return
		}
		sub, _ := claims["sub"].(string)
		role, _ := claims["role"].(string)
		if sub == "" || role == "" {
			httputil.Abort(c, http.StatusUnauthorized, httputil.CodeUnauthorized, "token sem sub ou role")
			return
		}
		c.Set(httputil.CtxUserID, sub)
		c.Set(httputil.CtxUserRole, role)
		if email, ok := claims["email"].(string); ok {
			c.Set(httputil.CtxUserEmail, email)
		}
		if name, ok := claims["name"].(string); ok {
			c.Set(httputil.CtxUserName, name)
		}
		c.Next()
	}
}

// RequireRole deixa passar só os papéis informados (403 para os demais, como o cliente).
func RequireRole(roles ...string) gin.HandlerFunc {
	allowed := make(map[string]bool, len(roles))
	for _, r := range roles {
		allowed[r] = true
	}
	return func(c *gin.Context) {
		if !allowed[c.GetString(httputil.CtxUserRole)] {
			httputil.Abort(c, http.StatusForbidden, httputil.CodeForbidden, "papel sem permissão para esta operação")
			return
		}
		c.Next()
	}
}
