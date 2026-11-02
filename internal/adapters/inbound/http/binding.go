package httputil

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
)

// O validator do gin passa a nomear o campo como no JSON (ou na query/URI), e não pelo nome
// do campo da struct Go. Precisa rodar antes da primeira validação de cada tipo, porque o
// validator guarda o nome em cache por tipo.
func init() {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterTagNameFunc(fieldTagName)
	}
}

func fieldTagName(f reflect.StructField) string {
	for _, tag := range []string{"json", "form", "uri"} {
		name, _, _ := strings.Cut(f.Tag.Get(tag), ",")
		if name != "" && name != "-" {
			return name
		}
	}
	return ""
}

// BindingMessage traduz o erro de ShouldBind* para uma mensagem que cita o campo e a regra,
// sem expor nomes de structs nem o texto interno do validator.
func BindingMessage(err error) string {
	var verrs validator.ValidationErrors
	var typeErr *json.UnmarshalTypeError
	var syntaxErr *json.SyntaxError
	switch {
	case errors.As(err, &verrs):
		parts := make([]string, 0, len(verrs))
		for _, fe := range verrs {
			parts = append(parts, fieldPath(fe)+": "+ruleMessage(fe))
		}
		return strings.Join(parts, "; ")
	case errors.As(err, &typeErr):
		return typeErr.Field + ": tipo inválido, esperado " + jsonType(typeErr.Type)
	case errors.As(err, &syntaxErr), errors.Is(err, io.ErrUnexpectedEOF):
		return "JSON malformado"
	case errors.Is(err, io.EOF):
		return "corpo vazio"
	}
	return err.Error()
}

// fieldPath tira o nome da struct raiz: "CreateRequest.items[0].quantity" vira "items[0].quantity".
func fieldPath(fe validator.FieldError) string {
	ns := fe.Namespace()
	if _, rest, ok := strings.Cut(ns, "."); ok {
		return rest
	}
	return ns
}

func ruleMessage(fe validator.FieldError) string {
	p := fe.Param()
	switch fe.Tag() {
	case "required":
		return "é obrigatório"
	case "email":
		return "deve ser um e-mail válido"
	case "uuid", "uuid4":
		return "deve ser um UUID"
	case "oneof":
		return "deve ser um destes: " + strings.ReplaceAll(p, " ", ", ")
	case "gt":
		return "deve ser maior que " + p
	case "gte":
		return "deve ser no mínimo " + p
	case "lt":
		return "deve ser menor que " + p
	case "lte":
		return "deve ser no máximo " + p
	case "min":
		return sizeMessage(fe.Kind(), "no mínimo", p)
	case "max":
		return sizeMessage(fe.Kind(), "no máximo", p)
	case "len":
		return sizeMessage(fe.Kind(), "exatamente", p)
	}
	return "é inválido (regra " + fe.Tag() + ")"
}

func sizeMessage(kind reflect.Kind, bound, p string) string {
	switch kind {
	case reflect.String:
		return "deve ter " + bound + " " + p + " caracteres"
	case reflect.Slice, reflect.Array, reflect.Map:
		return "deve ter " + bound + " " + p + " itens"
	default:
		return "deve ser " + bound + " " + p
	}
}

func jsonType(t reflect.Type) string {
	if t == nil {
		return "outro tipo"
	}
	switch t.Kind() {
	case reflect.String:
		return "texto"
	case reflect.Bool:
		return "booleano"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "número"
	case reflect.Slice, reflect.Array:
		return "lista"
	default:
		return "objeto"
	}
}
