package executionhandler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httputil "github.com/Kc1t/postech-tc4-execution-service/internal/adapters/inbound/http"
	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports/mocks"
	"github.com/gin-gonic/gin"
	"go.uber.org/mock/gomock"
)

func init() { gin.SetMode(gin.TestMode) }

type fixture struct {
	queue             *mocks.MockGetQueueUseCase
	get               *mocks.MockGetExecutionUseCase
	beginDiagnosis    *mocks.MockBeginDiagnosisUseCase
	completeDiagnosis *mocks.MockCompleteDiagnosisUseCase
	beginRepair       *mocks.MockBeginRepairUseCase
	finishRepair      *mocks.MockFinishRepairUseCase
	router            *gin.Engine
}

// newFixture monta o router com um middleware que simula o Auth e o CorrelationID.
func newFixture(t *testing.T, role string) *fixture {
	t.Helper()
	ctrl := gomock.NewController(t)
	f := &fixture{
		queue:             mocks.NewMockGetQueueUseCase(ctrl),
		get:               mocks.NewMockGetExecutionUseCase(ctrl),
		beginDiagnosis:    mocks.NewMockBeginDiagnosisUseCase(ctrl),
		completeDiagnosis: mocks.NewMockCompleteDiagnosisUseCase(ctrl),
		beginRepair:       mocks.NewMockBeginRepairUseCase(ctrl),
		finishRepair:      mocks.NewMockFinishRepairUseCase(ctrl),
	}
	h := NewHandler(UseCases{
		Queue: f.queue, Get: f.get, BeginDiagnosis: f.beginDiagnosis, CompleteDiagnosis: f.completeDiagnosis,
		BeginRepair: f.beginRepair, FinishRepair: f.finishRepair,
	})
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(httputil.CtxUserID, "user-7")
		c.Set(httputil.CtxUserRole, role)
		c.Set(httputil.CtxCorrelationID, "corr-77")
		c.Next()
	})
	h.SetupRoutes(r.Group("/api/v1"))
	f.router = r
	return f
}

func (f *fixture) do(method, path string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if s, ok := body.(string); ok {
		buf.WriteString(s)
	} else if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, req)
	return w
}

func errorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var resp httputil.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil || resp.Error.Code == "" {
		t.Fatalf("resposta de erro fora do padrão: %s", w.Body.String())
	}
	return resp.Error.Code
}

func errorMessage(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var resp httputil.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("resposta de erro fora do padrão: %s", w.Body.String())
	}
	return resp.Error.Message
}

var now = time.Date(2026, 11, 4, 8, 0, 0, 0, time.UTC)

func sampleExecution(status entities.Status) *entities.Execution {
	started := now.Add(time.Minute)
	return &entities.Execution{
		OSID: "9b2f3c4d-0001-4e5f-8a6b-000000000001", OSCode: 42, SagaID: "saga-1", Status: status,
		Vehicle:    entities.Vehicle{Plate: "ABC1D23", Brand: "Fiat", Model: "Uno", Year: 2020},
		Diagnosis:  &entities.Diagnosis{Services: []entities.DiagnosedService{{Code: 10}}, Parts: []entities.DiagnosedPart{{ManufacturerCode: "P-1", Quantity: 2}}, CompletedAt: now},
		Items:      []entities.RepairItem{{Kind: "part", Ref: "P-1", Description: "Peça", Quantity: 2, UnitPrice: 10.5}},
		EnqueuedAt: now, CreatedAt: now, UpdatedAt: now, DiagnosisStartedAt: &started, Version: 3,
	}
}

func TestQueue(t *testing.T) {
	f := newFixture(t, "mechanic")
	f.queue.EXPECT().Execute(gomock.Any(), ports.QueueQuery{}).Return([]*entities.Execution{
		sampleExecution(entities.StatusQueuedRepair), sampleExecution(entities.StatusQueuedDiagnosis),
	}, nil)

	w := f.do(http.MethodGet, "/api/v1/executions/queue", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var resp QueueResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Count != 2 || resp.Items[0].Status != "queued_repair" || resp.Items[0].Priority != entities.PriorityRepair ||
		resp.Items[1].Priority != entities.PriorityDiagnosis || resp.Items[0].Vehicle.Plate != "ABC1D23" {
		t.Fatalf("resposta errada: %+v", resp)
	}
}

func TestQueue_Filtros(t *testing.T) {
	f := newFixture(t, "admin")
	f.queue.EXPECT().Execute(gomock.Any(), ports.QueueQuery{Status: entities.StatusInRepair, Limit: 5}).Return(nil, nil)
	if w := f.do(http.MethodGet, "/api/v1/executions/queue?status=in_repair&limit=5", nil); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	w := f.do(http.MethodGet, "/api/v1/executions/queue?status=bogus", nil)
	if w.Code != http.StatusBadRequest || errorCode(t, w) != httputil.CodeInvalidStatus {
		t.Fatalf("status inválido: %d %s", w.Code, w.Body.String())
	}
	for _, l := range []string{"abc", "0", "-3"} {
		w := f.do(http.MethodGet, "/api/v1/executions/queue?limit="+l, nil)
		if w.Code != http.StatusBadRequest || errorCode(t, w) != httputil.CodeValidation {
			t.Fatalf("limit=%s: %d", l, w.Code)
		}
	}
}

func TestQueue_Erros(t *testing.T) {
	f := newFixture(t, "mechanic")
	f.queue.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, domainerrors.ErrInvalidStatus)
	if w := f.do(http.MethodGet, "/api/v1/executions/queue?status=finished", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("status %d", w.Code)
	}
	f.queue.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, errors.New("dynamodb fora"))
	w := f.do(http.MethodGet, "/api/v1/executions/queue", nil)
	if w.Code != http.StatusInternalServerError || errorCode(t, w) != httputil.CodeInternal {
		t.Fatalf("status %d", w.Code)
	}
}

func TestGet(t *testing.T) {
	f := newFixture(t, "mechanic")
	f.get.EXPECT().Execute(gomock.Any(), "9b2f3c4d-0001-4e5f-8a6b-000000000001").Return(&ports.ExecutionDetails{
		Execution: sampleExecution(entities.StatusInDiagnosis),
		Timeline: []entities.TimelineEvent{{
			ID: "e1", Type: entities.EventDiagnosisStarted, From: entities.StatusQueuedDiagnosis, To: entities.StatusInDiagnosis,
			Actor: entities.Actor{ID: "mec-1", Type: entities.ActorMechanic}, Details: map[string]string{"k": "v"}, OccurredAt: now,
		}},
	}, nil)

	w := f.do(http.MethodGet, "/api/v1/executions/9b2f3c4d-0001-4e5f-8a6b-000000000001", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var resp ExecutionDetailsResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.OSID != "9b2f3c4d-0001-4e5f-8a6b-000000000001" || resp.Version != 3 || resp.Diagnosis == nil || resp.Diagnosis.Parts[0].Quantity != 2 ||
		len(resp.Items) != 1 || resp.Items[0].UnitPrice != 10.5 || resp.DiagnosisStartedAt == nil {
		t.Fatalf("estado errado: %+v", resp)
	}
	if len(resp.Timeline) != 1 || resp.Timeline[0].FromStatus != "queued_diagnosis" || resp.Timeline[0].ActorType != "mechanic" || resp.Timeline[0].Details["k"] != "v" {
		t.Fatalf("linha do tempo errada: %+v", resp.Timeline)
	}
}

func TestGet_Erros(t *testing.T) {
	f := newFixture(t, "mechanic")
	f.get.EXPECT().Execute(gomock.Any(), "9b2f3c4d-0002-4e5f-8a6b-000000000002").Return(nil, domainerrors.ErrNotFound)
	w := f.do(http.MethodGet, "/api/v1/executions/9b2f3c4d-0002-4e5f-8a6b-000000000002", nil)
	if w.Code != http.StatusNotFound || errorCode(t, w) != httputil.CodeExecutionNotFound {
		t.Fatalf("status %d", w.Code)
	}
	// Sem EXPECT: id que não é UUID para no handler, antes do caso de uso e do DynamoDB.
	for _, bad := range []string{"nada", "123", strings.Repeat("a", 65), "9b2f3c4d-0002-4e5f-8a6b-00000000000"} {
		w = f.do(http.MethodGet, "/api/v1/executions/"+bad, nil)
		if w.Code != http.StatusBadRequest || errorCode(t, w) != httputil.CodeValidation {
			t.Fatalf("os_id %q: %d", bad, w.Code)
		}
		w = f.do(http.MethodPost, "/api/v1/executions/"+bad+"/diagnosis/start", nil)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("transição com os_id %q: %d", bad, w.Code)
		}
	}
	w = f.do(http.MethodGet, "/api/v1/executions/%20", nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("os_id vazio: %d", w.Code)
	}
}

// As quatro transições compartilham a forma: entrada com ator + correlação, 200 com o estado.
func TestTransicoes(t *testing.T) {
	type expectFn func(f *fixture, in ports.TransitionInput, out *entities.Execution, err error)
	cases := map[string]struct {
		path   string
		body   any
		expect expectFn
	}{
		"diagnosis/start": {"/diagnosis/start", nil, func(f *fixture, in ports.TransitionInput, out *entities.Execution, err error) {
			f.beginDiagnosis.EXPECT().Execute(gomock.Any(), in).Return(out, err)
		}},
		"repair/start": {"/repair/start", nil, func(f *fixture, in ports.TransitionInput, out *entities.Execution, err error) {
			f.beginRepair.EXPECT().Execute(gomock.Any(), in).Return(out, err)
		}},
		"repair/finish": {"/repair/finish", nil, func(f *fixture, in ports.TransitionInput, out *entities.Execution, err error) {
			f.finishRepair.EXPECT().Execute(gomock.Any(), in).Return(out, err)
		}},
		"diagnosis/complete": {"/diagnosis/complete", map[string]any{
			"services": []map[string]any{{"code": 10}},
			"parts":    []map[string]any{{"manufacturer_code": "P-1", "quantity": 2}},
			"notes":    "ok",
		}, func(f *fixture, in ports.TransitionInput, out *entities.Execution, err error) {
			f.completeDiagnosis.EXPECT().Execute(gomock.Any(), ports.CompleteDiagnosisInput{
				TransitionInput: in,
				Diagnosis: entities.Diagnosis{
					Services: []entities.DiagnosedService{{Code: 10}},
					Parts:    []entities.DiagnosedPart{{ManufacturerCode: "P-1", Quantity: 2}},
					Notes:    "ok",
				},
			}).Return(out, err)
		}},
	}
	errCases := []struct {
		err    error
		status int
		code   string
	}{
		{domainerrors.ErrNotFound, http.StatusNotFound, httputil.CodeExecutionNotFound},
		{domainerrors.ErrInvalidTransition, http.StatusConflict, httputil.CodeInvalidTransition},
		{domainerrors.ErrConcurrentUpdate, http.StatusConflict, httputil.CodeConcurrentUpdate},
		{errors.New("boom"), http.StatusInternalServerError, httputil.CodeInternal},
	}
	for name, c := range cases {
		for _, role := range []string{entities.ActorMechanic, entities.ActorAdmin} {
			t.Run(name+"/"+role, func(t *testing.T) {
				f := newFixture(t, role)
				in := ports.TransitionInput{OSID: "9b2f3c4d-0001-4e5f-8a6b-000000000001", Actor: entities.Actor{ID: "user-7", Type: role}, CorrelationID: "corr-77"}
				c.expect(f, in, sampleExecution(entities.StatusInRepair), nil)
				w := f.do(http.MethodPost, "/api/v1/executions/9b2f3c4d-0001-4e5f-8a6b-000000000001"+c.path, c.body)
				if w.Code != http.StatusOK {
					t.Fatalf("status %d: %s", w.Code, w.Body.String())
				}
				var resp ExecutionResponse
				_ = json.Unmarshal(w.Body.Bytes(), &resp)
				if resp.OSID != "9b2f3c4d-0001-4e5f-8a6b-000000000001" || resp.Status != "in_repair" {
					t.Fatalf("resposta errada: %+v", resp)
				}
			})
		}
		for _, ec := range errCases {
			t.Run(name+"/"+ec.code, func(t *testing.T) {
				f := newFixture(t, entities.ActorMechanic)
				in := ports.TransitionInput{OSID: "9b2f3c4d-0001-4e5f-8a6b-000000000001", Actor: entities.Actor{ID: "user-7", Type: entities.ActorMechanic}, CorrelationID: "corr-77"}
				c.expect(f, in, nil, ec.err)
				w := f.do(http.MethodPost, "/api/v1/executions/9b2f3c4d-0001-4e5f-8a6b-000000000001"+c.path, c.body)
				if w.Code != ec.status || errorCode(t, w) != ec.code {
					t.Fatalf("status %d code %s, esperava %d %s", w.Code, errorCode(t, w), ec.status, ec.code)
				}
			})
		}
		t.Run(name+"/os_id inválido", func(t *testing.T) {
			f := newFixture(t, entities.ActorMechanic)
			if w := f.do(http.MethodPost, "/api/v1/executions/%20"+c.path, c.body); w.Code != http.StatusBadRequest {
				t.Fatalf("status %d", w.Code)
			}
		})
	}
}

func TestCompleteDiagnosis_Validacao(t *testing.T) {
	cases := map[string]struct {
		body any
		want string
	}{
		"json inválido":       {"{", "corpo inválido: JSON malformado"},
		"código zero":         {map[string]any{"services": []map[string]any{{"code": 0}}}, "corpo inválido: services[0].code: é obrigatório"},
		"peça sem código":     {map[string]any{"parts": []map[string]any{{"quantity": 1}}}, "corpo inválido: parts[0].manufacturer_code: é obrigatório"},
		"quantidade negativa": {map[string]any{"parts": []map[string]any{{"manufacturer_code": "X", "quantity": -1}}}, "corpo inválido: parts[0].quantity: deve ser maior que 0"},
		"tipo errado":         {map[string]any{"services": "x"}, "corpo inválido: services: tipo inválido, esperado lista"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, entities.ActorMechanic)
			w := f.do(http.MethodPost, "/api/v1/executions/9b2f3c4d-0001-4e5f-8a6b-000000000001/diagnosis/complete", tc.body)
			if w.Code != http.StatusBadRequest || errorCode(t, w) != httputil.CodeValidation {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if msg := errorMessage(t, w); msg != tc.want {
				t.Errorf("mensagem: esperava %q, veio %q", tc.want, msg)
			}
		})
	}
	t.Run("diagnóstico vazio vira 422 pelo domínio", func(t *testing.T) {
		f := newFixture(t, entities.ActorMechanic)
		f.completeDiagnosis.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(nil, domainerrors.ErrInvalidDiagnosis)
		w := f.do(http.MethodPost, "/api/v1/executions/9b2f3c4d-0001-4e5f-8a6b-000000000001/diagnosis/complete", map[string]any{"notes": "nada"})
		if w.Code != http.StatusUnprocessableEntity || errorCode(t, w) != httputil.CodeInvalidDiagnosis {
			t.Fatalf("status %d", w.Code)
		}
	})
}
