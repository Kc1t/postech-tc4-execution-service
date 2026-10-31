package executionuc

import (
	"context"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
)

// FinishRepair: o mecânico conclui o reparo (→ RepairFinished). A saga libera a entrega.
type FinishRepair struct{ base }

func NewFinishRepair(d Deps) *FinishRepair { return &FinishRepair{newBase(d)} }

func (uc *FinishRepair) Execute(ctx context.Context, in ports.TransitionInput) (*entities.Execution, error) {
	return uc.transition(ctx, in, contracts.EvtRepairFinished, func(e *entities.Execution, now time.Time) (any, error) {
		if err := e.FinishRepair(in.Actor, now); err != nil {
			return nil, err
		}
		return contracts.RepairFinished{FinishedAt: now}, nil
	})
}
