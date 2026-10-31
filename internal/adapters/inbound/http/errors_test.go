package httputil

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

func serve(h gin.HandlerFunc) *httptest.ResponseRecorder {
	r := gin.New()
	r.GET("/", h)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	return w
}

func body(t *testing.T, w *httptest.ResponseRecorder) ErrorBody {
	t.Helper()
	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("corpo não segue o formato padrão: %s", w.Body.String())
	}
	return resp.Error
}

func TestHandleError(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("%w: os_id=1", domainerrors.ErrNotFound), http.StatusNotFound, CodeExecutionNotFound},
		{domainerrors.ErrConcurrentUpdate, http.StatusConflict, CodeConcurrentUpdate},
		{domainerrors.ErrInvalidTransition, http.StatusConflict, CodeInvalidTransition},
		{domainerrors.ErrInvalidDiagnosis, http.StatusUnprocessableEntity, CodeInvalidDiagnosis},
		{domainerrors.ErrInvalidStatus, http.StatusBadRequest, CodeInvalidStatus},
		{domainerrors.ErrInvalidExecution, http.StatusBadRequest, CodeValidation},
		{errors.New("dynamodb fora"), http.StatusInternalServerError, CodeInternal},
	}
	for _, c := range cases {
		t.Run(c.code, func(t *testing.T) {
			w := serve(func(ctx *gin.Context) { HandleError(ctx, c.err) })
			if w.Code != c.status {
				t.Fatalf("status = %d, esperava %d", w.Code, c.status)
			}
			b := body(t, w)
			if b.Code != c.code {
				t.Fatalf("code = %s, esperava %s", b.Code, c.code)
			}
			if c.status == http.StatusInternalServerError && b.Message != "erro interno" {
				t.Fatalf("500 não pode vazar detalhe interno: %q", b.Message)
			}
			if c.status != http.StatusInternalServerError && b.Message != c.err.Error() {
				t.Fatalf("mensagem = %q", b.Message)
			}
		})
	}
}

func TestBadRequest(t *testing.T) {
	w := serve(func(ctx *gin.Context) { BadRequest(ctx, "campo x obrigatório") })
	if b := body(t, w); w.Code != http.StatusBadRequest || b.Code != CodeValidation || b.Message != "campo x obrigatório" {
		t.Fatalf("status=%d body=%+v", w.Code, b)
	}
}
