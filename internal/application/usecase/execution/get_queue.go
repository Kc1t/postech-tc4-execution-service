package executionuc

import (
	"context"
	"fmt"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
)

const (
	DefaultQueueLimit = 50
	MaxQueueLimit     = 200
)

// workQueue é a fila padrão do painel: reparo primeiro (orçamento já pago), depois diagnóstico.
var workQueue = []entities.Status{entities.StatusQueuedRepair, entities.StatusQueuedDiagnosis}

// GetQueue lista a fila de trabalho do mecânico, mais antigas primeiro em cada raia.
type GetQueue struct{ repo ports.ExecutionRepository }

func NewGetQueue(repo ports.ExecutionRepository) *GetQueue { return &GetQueue{repo: repo} }

func (uc *GetQueue) Execute(ctx context.Context, q ports.QueueQuery) ([]*entities.Execution, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultQueueLimit
	}
	limit = min(limit, MaxQueueLimit)

	if q.Status != "" {
		if !q.Status.IsActive() {
			return nil, fmt.Errorf("%w: a fila só lista status ativos (%s)", domainerrors.ErrInvalidStatus, q.Status)
		}
		return uc.repo.ListByStatus(ctx, q.Status, limit)
	}

	out := make([]*entities.Execution, 0)
	for _, st := range workQueue {
		if len(out) >= limit {
			break
		}
		items, err := uc.repo.ListByStatus(ctx, st, limit-len(out))
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}
