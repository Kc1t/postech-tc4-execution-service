package executionuc

import (
	"context"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
)

// BeginDiagnosis: o mecânico tira o veículo da fila de diagnóstico (→ DiagnosisStarted).
type BeginDiagnosis struct{ base }

func NewBeginDiagnosis(d Deps) *BeginDiagnosis { return &BeginDiagnosis{newBase(d)} }

func (uc *BeginDiagnosis) Execute(ctx context.Context, in ports.TransitionInput) (*entities.Execution, error) {
	return uc.transition(ctx, in, contracts.EvtDiagnosisStarted, func(e *entities.Execution, now time.Time) (any, error) {
		if err := e.BeginDiagnosis(in.Actor, now); err != nil {
			return nil, err
		}
		return contracts.DiagnosisStarted{StartedAt: now}, nil
	})
}
