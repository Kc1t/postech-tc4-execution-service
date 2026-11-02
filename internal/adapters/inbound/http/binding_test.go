package httputil

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type bindingItem struct {
	Quantity int `json:"quantity" binding:"gt=0"`
}

type bindingRequest struct {
	Name   string        `json:"name" binding:"required,max=5"`
	Email  string        `json:"email" binding:"omitempty,email"`
	Year   int           `json:"year" binding:"omitempty,min=1900,lte=2100"`
	Status string        `json:"status" binding:"omitempty,oneof=open closed"`
	Tags   []string      `json:"tags" binding:"omitempty,min=2"`
	Items  []bindingItem `json:"items" binding:"omitempty,dive"`
}

type bindingQuery struct {
	Document string `form:"document" binding:"required"`
}

// bindMessage faz o bind do corpo num handler do gin e devolve a mensagem de BindingMessage.
func bindMessage(t *testing.T, body string) string {
	t.Helper()
	var msg string
	r := gin.New()
	r.POST("/", func(c *gin.Context) {
		var req bindingRequest
		err := c.ShouldBindJSON(&req)
		if err == nil {
			t.Fatalf("%s: esperava erro de bind", body)
		}
		msg = BindingMessage(err)
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
	return msg
}

func TestBindingMessage_CitaCampoDoJSONERegra(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{}`, "name: é obrigatório"},
		{`{"name":"abcdefg"}`, "name: deve ter no máximo 5 caracteres"},
		{`{"name":"a","email":"x"}`, "email: deve ser um e-mail válido"},
		{`{"name":"a","year":1800}`, "year: deve ser no mínimo 1900"},
		{`{"name":"a","year":2200}`, "year: deve ser no máximo 2100"},
		{`{"name":"a","status":"x"}`, "status: deve ser um destes: open, closed"},
		{`{"name":"a","tags":["x"]}`, "tags: deve ter no mínimo 2 itens"},
		{`{"name":"a","items":[{"quantity":0}]}`, "items[0].quantity: deve ser maior que 0"},
		{`{"name":"a","year":"2020"}`, "year: tipo inválido, esperado número"},
		{`{"email":"x"}`, "name: é obrigatório; email: deve ser um e-mail válido"},
		{`{"name":`, "JSON malformado"},
		{`{bad`, "JSON malformado"},
		{``, "corpo vazio"},
	}
	for _, tc := range cases {
		got := bindMessage(t, tc.body)
		if got != tc.want {
			t.Errorf("%s: esperava %q, veio %q", tc.body, tc.want, got)
		}
		if strings.Contains(got, "bindingRequest") || strings.Contains(got, "Error:Field") {
			t.Errorf("%s: mensagem expõe o validator: %q", tc.body, got)
		}
	}
}

func TestBindingMessage_QueryEOutrosErros(t *testing.T) {
	var msg string
	r := gin.New()
	r.GET("/", func(c *gin.Context) {
		var q bindingQuery
		if err := c.ShouldBindQuery(&q); err != nil {
			msg = BindingMessage(err)
		}
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if msg != "document: é obrigatório" {
		t.Errorf("query: mensagem inesperada %q", msg)
	}

	if got := BindingMessage(errors.New("outro erro")); got != "outro erro" {
		t.Errorf("erro comum deve passar direto, veio %q", got)
	}
}
