package entities

import (
	"fmt"
	"strings"
	"time"

	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
)

// DiagnosedService é um serviço do catálogo indicado no diagnóstico (código do os-service).
type DiagnosedService struct {
	Code int
}

// DiagnosedPart é uma peça indicada no diagnóstico (código do fabricante e quantidade).
type DiagnosedPart struct {
	ManufacturerCode string
	Quantity         int
}

// Diagnosis é o resultado do diagnóstico. Os preços não ficam aqui: o os-service monta o
// orçamento a partir do catálogo.
type Diagnosis struct {
	Services    []DiagnosedService
	Parts       []DiagnosedPart
	Notes       string
	CompletedAt time.Time
}

// Validate exige pelo menos um serviço ou peça, códigos preenchidos e quantidades positivas.
func (d Diagnosis) Validate() error {
	if len(d.Services) == 0 && len(d.Parts) == 0 {
		return fmt.Errorf("%w: informe ao menos um serviço ou peça", domainerrors.ErrInvalidDiagnosis)
	}
	for i, s := range d.Services {
		if s.Code <= 0 {
			return fmt.Errorf("%w: services[%d].code deve ser positivo", domainerrors.ErrInvalidDiagnosis, i)
		}
	}
	for i, p := range d.Parts {
		if strings.TrimSpace(p.ManufacturerCode) == "" {
			return fmt.Errorf("%w: parts[%d].manufacturer_code obrigatório", domainerrors.ErrInvalidDiagnosis, i)
		}
		if p.Quantity <= 0 {
			return fmt.Errorf("%w: parts[%d].quantity deve ser positiva", domainerrors.ErrInvalidDiagnosis, i)
		}
	}
	return nil
}
