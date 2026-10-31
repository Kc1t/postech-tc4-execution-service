package routes

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	executionhandler "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http/execution"
	healthhandler "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http/health"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports/mocks"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/mock/gomock"
)

func init() { gin.SetMode(gin.TestMode) }

func router(t *testing.T, nr *newrelic.Application) (*gin.Engine, *mocks.MockGetQueueUseCase) {
	ctrl := gomock.NewController(t)
	queue := mocks.NewMockGetQueueUseCase(ctrl)
	h := executionhandler.NewHandler(executionhandler.UseCases{
		Queue: queue, Get: mocks.NewMockGetExecutionUseCase(ctrl),
		BeginDiagnosis: mocks.NewMockBeginDiagnosisUseCase(ctrl), CompleteDiagnosis: mocks.NewMockCompleteDiagnosisUseCase(ctrl),
		BeginRepair: mocks.NewMockBeginRepairUseCase(ctrl), FinishRepair: mocks.NewMockFinishRepairUseCase(ctrl),
	})
	r := New(Deps{
		Log:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
		NewRelic:    nr,
		JWTSecret:   "s",
		CORSOrigins: []string{"https://painel.oficina.com"},
		Executions:  h,
		Health:      healthhandler.NewHandler(map[string]healthhandler.Checker{"dynamodb": healthhandler.CheckerFunc(func(context.Context) error { return nil })}),
	})
	r.GET("/panic", func(*gin.Context) { panic("boom") })
	return r, queue
}

func get(r *gin.Engine, path, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func errCode(w *httptest.ResponseRecorder) string {
	var resp httputil.ErrorResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	return resp.Error.Code
}

func TestRouter(t *testing.T) {
	nr, _ := newrelic.NewApplication(newrelic.ConfigAppName("t"), newrelic.ConfigEnabled(false))
	r, queue := router(t, nr)

	if w := get(r, "/health/live", ""); w.Code != http.StatusOK || w.Header().Get("X-Correlation-ID") == "" {
		t.Fatalf("live: %d", w.Code)
	}
	if w := get(r, "/health/ready", ""); w.Code != http.StatusOK {
		t.Fatalf("ready: %d", w.Code)
	}
	if w := get(r, "/swagger/index.html", ""); w.Code != http.StatusOK {
		t.Fatalf("swagger: %d", w.Code)
	}
	if w := get(r, "/api/v1/executions/queue", ""); w.Code != http.StatusUnauthorized || errCode(w) != httputil.CodeUnauthorized {
		t.Fatalf("API exige JWT: %d", w.Code)
	}

	tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "u", "role": "mechanic", "exp": time.Now().Add(time.Hour).Unix()}).SignedString([]byte("s"))
	queue.EXPECT().Execute(gomock.Any(), ports.QueueQuery{}).Return(nil, nil)
	if w := get(r, "/api/v1/executions/queue", tok); w.Code != http.StatusOK {
		t.Fatalf("queue autenticada: %d %s", w.Code, w.Body.String())
	}

	if w := get(r, "/nao-existe", ""); w.Code != http.StatusNotFound || errCode(w) != httputil.CodeNotFound {
		t.Fatalf("404 padronizado: %d %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest(http.MethodDelete, "/health/live", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed || errCode(w) != httputil.CodeMethodNotAllowed {
		t.Fatalf("405 padronizado: %d", w.Code)
	}
	if w := get(r, "/panic", ""); w.Code != http.StatusInternalServerError || errCode(w) != httputil.CodeInternal {
		t.Fatalf("pânico vira 500 padronizado: %d", w.Code)
	}
}

func TestRouter_SemNewRelic(t *testing.T) {
	r, _ := router(t, nil)
	if w := get(r, "/health/live", ""); w.Code != http.StatusOK {
		t.Fatalf("live sem agente: %d", w.Code)
	}
}
