package middleware

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/newrelic/go-agent/v3/integrations/nrgin"
	"github.com/newrelic/go-agent/v3/newrelic"
)

func init() { gin.SetMode(gin.TestMode) }

const secret = "segredo-de-teste"

func token(t *testing.T, method jwt.SigningMethod, key any, claims jwt.MapClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(method, claims).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func claims(role string) jwt.MapClaims {
	return jwt.MapClaims{"sub": "user-1", "role": role, "email": "m@oficina.com", "name": "Mecânico", "exp": time.Now().Add(time.Hour).Unix()}
}

func serve(r *gin.Engine, method, path string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func code(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var resp httputil.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("erro fora do padrão: %s", w.Body.String())
	}
	return resp.Error.Code
}

func authRouter() *gin.Engine {
	r := gin.New()
	r.Use(Auth(secret), RequireRole("admin", "mechanic"))
	r.GET("/x", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"sub": c.GetString(httputil.CtxUserID), "role": c.GetString(httputil.CtxUserRole),
			"email": c.GetString(httputil.CtxUserEmail), "name": c.GetString(httputil.CtxUserName)})
	})
	return r
}

func TestAuth_AceitaAdminEMecanico(t *testing.T) {
	for _, role := range []string{"admin", "mechanic"} {
		w := serve(authRouter(), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token(t, jwt.SigningMethodHS256, []byte(secret), claims(role))})
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"role":"`+role+`"`) || !strings.Contains(w.Body.String(), `"sub":"user-1"`) {
			t.Fatalf("%s: %d %s", role, w.Code, w.Body.String())
		}
	}
}

func TestAuth_ClienteRecebe403(t *testing.T) {
	c := claims("client")
	c["document"] = "12345678909"
	w := serve(authRouter(), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token(t, jwt.SigningMethodHS256, []byte(secret), c)})
	if w.Code != http.StatusForbidden || code(t, w) != httputil.CodeForbidden {
		t.Fatalf("cliente: %d", w.Code)
	}
}

func TestAuth_Recusas(t *testing.T) {
	expired := claims("admin")
	expired["exp"] = time.Now().Add(-time.Minute).Unix()
	noExp := claims("admin")
	delete(noExp, "exp")
	noRole := claims("")
	cases := map[string]string{
		"sem header":      "",
		"sem bearer":      token(t, jwt.SigningMethodHS256, []byte(secret), claims("admin")),
		"bearer vazio":    "Bearer   ",
		"lixo":            "Bearer a.b.c",
		"segredo errado":  "Bearer " + token(t, jwt.SigningMethodHS256, []byte("outro"), claims("admin")),
		"expirado":        "Bearer " + token(t, jwt.SigningMethodHS256, []byte(secret), expired),
		"sem exp":         "Bearer " + token(t, jwt.SigningMethodHS256, []byte(secret), noExp),
		"sem role":        "Bearer " + token(t, jwt.SigningMethodHS256, []byte(secret), noRole),
		"algoritmo HS512": "Bearer " + token(t, jwt.SigningMethodHS512, []byte(secret), claims("admin")),
		"algoritmo none":  "Bearer " + token(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims("admin")),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			headers := map[string]string{}
			if h != "" {
				headers["Authorization"] = h
			}
			w := serve(authRouter(), http.MethodGet, "/x", headers)
			if w.Code != http.StatusUnauthorized || code(t, w) != httputil.CodeUnauthorized {
				t.Fatalf("esperava 401, veio %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestCorrelationID(t *testing.T) {
	r := gin.New()
	r.Use(CorrelationID())
	r.GET("/x", func(c *gin.Context) { c.String(http.StatusOK, c.GetString(httputil.CtxCorrelationID)) })

	w := serve(r, http.MethodGet, "/x", map[string]string{CorrelationHeader: "abc-123"})
	if w.Body.String() != "abc-123" || w.Header().Get(CorrelationHeader) != "abc-123" {
		t.Fatalf("deve reaproveitar o header: %q %q", w.Body.String(), w.Header().Get(CorrelationHeader))
	}
	w = serve(r, http.MethodGet, "/x", map[string]string{"X-Request-ID": "req-9"})
	if w.Body.String() != "req-9" {
		t.Fatalf("deve aceitar X-Request-ID: %q", w.Body.String())
	}
	for _, bad := range []string{"", "tem espaço", "quebra\nde linha", strings.Repeat("a", maxCorrelationLen+1)} {
		w = serve(r, http.MethodGet, "/x", map[string]string{CorrelationHeader: bad})
		if got := w.Header().Get(CorrelationHeader); got == bad || len(got) != 32 {
			t.Fatalf("valor %q deveria ser trocado por um gerado, veio %q", bad, got)
		}
	}
}

func TestRequestLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	base := slog.New(slog.NewJSONHandler(buf, nil))
	r := gin.New()
	r.Use(CorrelationID(), RequestLogger(base, "/health/live"))
	r.GET("/health/live", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/ok", func(c *gin.Context) {
		c.Set(httputil.CtxUserID, "user-1")
		logger.FromContext(c.Request.Context()).Info("dentro do handler")
		c.Status(http.StatusOK)
	})
	r.GET("/warn", func(c *gin.Context) { c.Status(http.StatusConflict) })
	r.GET("/err", func(c *gin.Context) { _ = c.Error(http.ErrAbortHandler); c.Status(http.StatusInternalServerError) })

	serve(r, http.MethodGet, "/health/live", nil)
	if buf.Len() != 0 {
		t.Fatalf("probe não gera log de acesso: %s", buf.String())
	}
	serve(r, http.MethodGet, "/ok", map[string]string{CorrelationHeader: "c-1"})
	out := buf.String()
	if !strings.Contains(out, `"msg":"dentro do handler"`) || !strings.Contains(out, `"correlation_id":"c-1"`) ||
		!strings.Contains(out, `"user_id":"user-1"`) || !strings.Contains(out, `"route":"/ok"`) {
		t.Fatalf("log sem os campos esperados: %s", out)
	}
	buf.Reset()
	serve(r, http.MethodGet, "/warn", nil)
	serve(r, http.MethodGet, "/err", nil)
	if !strings.Contains(buf.String(), `"level":"WARN"`) || !strings.Contains(buf.String(), `"level":"ERROR"`) || !strings.Contains(buf.String(), `"errors"`) {
		t.Fatalf("níveis por status errados: %s", buf.String())
	}
}

func TestNewRelicContext(t *testing.T) {
	app, _ := newrelic.NewApplication(newrelic.ConfigAppName("t"), newrelic.ConfigEnabled(false))
	r := gin.New()
	r.Use(nrgin.Middleware(app), NewRelicContext())
	r.GET("/x", func(c *gin.Context) {
		if newrelic.FromContext(c.Request.Context()) == nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		c.Status(http.StatusOK)
	})
	if w := serve(r, http.MethodGet, "/x", nil); w.Code != http.StatusOK {
		t.Fatal("a transação do nrgin deve estar no contexto da requisição")
	}

	plain := gin.New()
	plain.Use(NewRelicContext())
	plain.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })
	if w := serve(plain, http.MethodGet, "/x", nil); w.Code != http.StatusOK {
		t.Fatal("sem agente o middleware não interfere")
	}
}

func TestCORS(t *testing.T) {
	r := gin.New()
	r.Use(CORS([]string{"https://painel.oficina.com/"}))
	r.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.OPTIONS("/x", func(c *gin.Context) { c.Status(http.StatusTeapot) })

	w := serve(r, http.MethodGet, "/x", map[string]string{"Origin": "https://painel.oficina.com"})
	if w.Header().Get("Access-Control-Allow-Origin") != "https://painel.oficina.com" || w.Header().Get("Vary") != "Origin" {
		t.Fatalf("origem permitida sem headers: %v", w.Header())
	}
	w = serve(r, http.MethodGet, "/x", map[string]string{"Origin": "https://mal.com"})
	if w.Code != http.StatusOK || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("origem não listada não recebe Allow-Origin: %v", w.Header())
	}
	w = serve(r, http.MethodOptions, "/x", map[string]string{"Origin": "https://painel.oficina.com"})
	if w.Code != http.StatusNoContent || !strings.Contains(w.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("preflight permitido: %d %v", w.Code, w.Header())
	}
	w = serve(r, http.MethodOptions, "/x", map[string]string{"Origin": "https://mal.com"})
	if w.Code != http.StatusForbidden || code(t, w) != httputil.CodeOriginNotAllowed {
		t.Fatalf("preflight negado: %d", w.Code)
	}
	if w = serve(r, http.MethodGet, "/x", nil); w.Header().Get("Access-Control-Allow-Origin") != "" || w.Code != http.StatusOK {
		t.Fatal("sem Origin não há CORS")
	}
}
