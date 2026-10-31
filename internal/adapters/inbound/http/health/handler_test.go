package healthhandler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

func call(h *Handler, path string) *httptest.ResponseRecorder {
	r := gin.New()
	h.SetupRoutes(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestLive(t *testing.T) {
	h := NewHandler(map[string]Checker{"dynamodb": CheckerFunc(func(context.Context) error { return errors.New("fora") })})
	if w := call(h, "/health/live"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Fatalf("liveness não depende das dependências: %d %s", w.Code, w.Body.String())
	}
}

func TestReady(t *testing.T) {
	h := NewHandler(map[string]Checker{"dynamodb": CheckerFunc(func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("o check precisa de timeout")
		}
		return nil
	})})
	w := call(h, "/health/ready")
	var resp Response
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if w.Code != http.StatusOK || resp.Status != "ready" || resp.Checks["dynamodb"] != "ok" {
		t.Fatalf("ready: %d %s", w.Code, w.Body.String())
	}
}

func TestReady_DependenciaFora(t *testing.T) {
	h := NewHandler(map[string]Checker{
		"a":        CheckerFunc(func(context.Context) error { return nil }),
		"dynamodb": CheckerFunc(func(context.Context) error { return errors.New("tabela inexistente") }),
	})
	w := call(h, "/health/ready")
	var resp httputil.ErrorResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if w.Code != http.StatusServiceUnavailable || resp.Error.Code != httputil.CodeServiceUnavailable || !strings.Contains(resp.Error.Message, "dynamodb") {
		t.Fatalf("ready com falha: %d %s", w.Code, w.Body.String())
	}
}
