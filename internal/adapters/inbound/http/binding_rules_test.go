package httputil

import (
	"reflect"
	"testing"

	"github.com/go-playground/validator/v10"
)

// fakeFieldError implementa só o que ruleMessage e fieldPath leem do validator.FieldError.
type fakeFieldError struct {
	validator.FieldError
	ns, tag, param string
	kind           reflect.Kind
}

func (f fakeFieldError) Namespace() string  { return f.ns }
func (f fakeFieldError) Tag() string        { return f.tag }
func (f fakeFieldError) Param() string      { return f.param }
func (f fakeFieldError) Kind() reflect.Kind { return f.kind }

func TestRuleMessage_TodasAsRegras(t *testing.T) {
	cases := []struct {
		tag, param string
		kind       reflect.Kind
		want       string
	}{
		{"uuid", "", reflect.String, "deve ser um UUID"},
		{"gte", "1", reflect.Int, "deve ser no mínimo 1"},
		{"lt", "10", reflect.Int, "deve ser menor que 10"},
		{"len", "8", reflect.String, "deve ter exatamente 8 caracteres"},
		{"len", "3", reflect.Int, "deve ser exatamente 3"},
		{"max", "2", reflect.Map, "deve ter no máximo 2 itens"},
		{"url", "", reflect.String, "é inválido (regra url)"},
	}
	for _, tc := range cases {
		got := ruleMessage(fakeFieldError{tag: tc.tag, param: tc.param, kind: tc.kind})
		if got != tc.want {
			t.Errorf("%s=%s: esperava %q, veio %q", tc.tag, tc.param, tc.want, got)
		}
	}
}

func TestFieldPath_SemStructRaiz(t *testing.T) {
	if got := fieldPath(fakeFieldError{ns: "document"}); got != "document" {
		t.Errorf("esperava document, veio %q", got)
	}
	if got := fieldPath(fakeFieldError{ns: "Request.items[0].code"}); got != "items[0].code" {
		t.Errorf("esperava items[0].code, veio %q", got)
	}
}

func TestJSONType(t *testing.T) {
	cases := []struct {
		typ  reflect.Type
		want string
	}{
		{reflect.TypeOf(""), "texto"},
		{reflect.TypeOf(true), "booleano"},
		{reflect.TypeOf(1.5), "número"},
		{reflect.TypeOf([]int{}), "lista"},
		{reflect.TypeOf(struct{}{}), "objeto"},
		{nil, "outro tipo"},
	}
	for _, tc := range cases {
		if got := jsonType(tc.typ); got != tc.want {
			t.Errorf("%v: esperava %q, veio %q", tc.typ, tc.want, got)
		}
	}
}

func TestFieldTagName(t *testing.T) {
	type sample struct {
		JSON    string `json:"name,omitempty"`
		Form    string `form:"document"`
		URI     string `uri:"id"`
		Ignored string `json:"-"`
		Plain   string
	}
	typ := reflect.TypeOf(sample{})
	want := []string{"name", "document", "id", "", ""}
	for i, w := range want {
		if got := fieldTagName(typ.Field(i)); got != w {
			t.Errorf("%s: esperava %q, veio %q", typ.Field(i).Name, w, got)
		}
	}
}
