package executionuc

import (
	"context"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
)

// BeginRepair: o mecânico tira o veículo da fila de reparo (→ RepairStarted).
type BeginRepair struct{ base }

func NewBeginRepair(d Deps) *BeginRepair { return &BeginRepair{newBase(d)} }

func (uc *BeginRepair) Execute(ctx context.Context, in ports.TransitionInput) (*entities.Execution, error) {
	return uc.transition(ctx, in, contracts.EvtRepairStarted, func(e *entities.Execution, now time.Time) (any, error) {
		if err := e.BeginRepair(in.Actor, now); err != nil {
			return nil, err
		}
		return contracts.RepairStarted{StartedAt: now}, nil
	})
}
