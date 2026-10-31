package ports

import (
	"context"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
)

//go:generate mockgen -source=./usecases.go -destination=./mocks/usecases.go -package=mocks

// MessageMeta identifica a mensagem (envelope) que originou um comando.
type MessageMeta struct {
	MessageID     string
	Type          string
	SagaID        string
	OSID          string
	CorrelationID string
	Producer      string
}

// ---- Comandos da saga (fila execution-commands) ----

type StartDiagnosisInput struct {
	Message MessageMeta
	OSCode  int
	Vehicle entities.Vehicle
	Notes   string
}

type StartRepairInput struct {
	Message         MessageMeta
	Items           []entities.RepairItem
	SimulateFailure string
}

type CancelExecutionInput struct {
	Message MessageMeta
	Reason  string
}

// StartDiagnosisUseCase abre a execução na fila de diagnóstico (idempotente por os_id).
type StartDiagnosisUseCase interface {
	Execute(ctx context.Context, in StartDiagnosisInput) error
}

// StartRepairUseCase coloca a execução diagnosticada na fila de reparo ou a recusa.
type StartRepairUseCase interface {
	Execute(ctx context.Context, in StartRepairInput) error
}

// CancelExecutionUseCase cancela a execução (compensação da saga).
type CancelExecutionUseCase interface {
	Execute(ctx context.Context, in CancelExecutionInput) error
}

// ---- API do mecânico ----

// TransitionInput identifica a execução e quem age sobre ela.
type TransitionInput struct {
	OSID          string
	Actor         entities.Actor
	CorrelationID string
}

type CompleteDiagnosisInput struct {
	TransitionInput
	Diagnosis entities.Diagnosis
}

// QueueQuery filtra a fila de trabalho. Status vazio = fila padrão (reparo e depois diagnóstico).
type QueueQuery struct {
	Status entities.Status
	Limit  int
}

// ExecutionDetails é a execução com a linha do tempo.
type ExecutionDetails struct {
	Execution *entities.Execution
	Timeline  []entities.TimelineEvent
}

type GetQueueUseCase interface {
	Execute(ctx context.Context, q QueueQuery) ([]*entities.Execution, error)
}

type GetExecutionUseCase interface {
	Execute(ctx context.Context, osID string) (*ExecutionDetails, error)
}

type BeginDiagnosisUseCase interface {
	Execute(ctx context.Context, in TransitionInput) (*entities.Execution, error)
}

type CompleteDiagnosisUseCase interface {
	Execute(ctx context.Context, in CompleteDiagnosisInput) (*entities.Execution, error)
}

type BeginRepairUseCase interface {
	Execute(ctx context.Context, in TransitionInput) (*entities.Execution, error)
}

type FinishRepairUseCase interface {
	Execute(ctx context.Context, in TransitionInput) (*entities.Execution, error)
}
