package executionuc

import (
	"context"

	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
)

// GetExecution devolve o estado e a linha do tempo de uma execução. Também é usado pela API
// composition do os-service.
type GetExecution struct{ repo ports.ExecutionRepository }

func NewGetExecution(repo ports.ExecutionRepository) *GetExecution { return &GetExecution{repo: repo} }

func (uc *GetExecution) Execute(ctx context.Context, osID string) (*ports.ExecutionDetails, error) {
	exec, timeline, err := uc.repo.GetWithTimeline(ctx, osID)
	if err != nil {
		return nil, err
	}
	return &ports.ExecutionDetails{Execution: exec, Timeline: timeline}, nil
}
