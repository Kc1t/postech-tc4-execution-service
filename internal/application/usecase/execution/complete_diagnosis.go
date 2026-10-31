package executionuc

import (
	"context"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
)

// CompleteDiagnosis: o mecânico registra serviços e peças (→ DiagnosisCompleted). A saga usa o
// evento para reservar peças e pedir o orçamento ao billing-service.
type CompleteDiagnosis struct{ base }

func NewCompleteDiagnosis(d Deps) *CompleteDiagnosis { return &CompleteDiagnosis{newBase(d)} }

func (uc *CompleteDiagnosis) Execute(ctx context.Context, in ports.CompleteDiagnosisInput) (*entities.Execution, error) {
	return uc.transition(ctx, in.TransitionInput, contracts.EvtDiagnosisCompleted, func(e *entities.Execution, now time.Time) (any, error) {
		if err := e.CompleteDiagnosis(in.Diagnosis, in.Actor, now); err != nil {
			return nil, err
		}
		return toDiagnosisCompleted(*e.Diagnosis), nil
	})
}

func toDiagnosisCompleted(d entities.Diagnosis) contracts.DiagnosisCompleted {
	out := contracts.DiagnosisCompleted{
		Services: make([]contracts.DiagnosedService, 0, len(d.Services)),
		Parts:    make([]contracts.DiagnosedPart, 0, len(d.Parts)),
		Notes:    d.Notes,
	}
	for _, s := range d.Services {
		out.Services = append(out.Services, contracts.DiagnosedService{Code: s.Code})
	}
	for _, p := range d.Parts {
		out.Parts = append(out.Parts, contracts.DiagnosedPart{ManufacturerCode: p.ManufacturerCode, Quantity: p.Quantity})
	}
	return out
}
