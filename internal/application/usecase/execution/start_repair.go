package executionuc

import (
	"context"
	"errors"
	"fmt"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
)

// DefaultMaxConcurrentRepairs é a capacidade padrão da oficina (MAX_CONCURRENT_REPAIRS).
const DefaultMaxConcurrentRepairs = 10

// ReasonSimulatedFailure é o motivo da recusa forçada por FAULT_INJECTION_ENABLED.
const ReasonSimulatedFailure = "falha simulada: reparo recusado (simulate_failure=repair)"

// StartRepairConfig ajusta as regras de aceite do reparo.
type StartRepairConfig struct {
	// MaxConcurrentRepairs é o máximo de execuções em queued_repair + in_repair.
	MaxConcurrentRepairs int
	// FaultInjectionEnabled faz simulate_failure=repair recusar o reparo.
	FaultInjectionEnabled bool
}

// StartRepair trata o comando execution.StartRepair (orçamento pago): coloca a execução
// diagnosticada na fila de reparo. Regra de negócio violada não é erro: vira RepairRejected,
// e a saga compensa.
type StartRepair struct {
	base
	cfg StartRepairConfig
}

func NewStartRepair(d Deps, cfg StartRepairConfig) *StartRepair {
	if cfg.MaxConcurrentRepairs <= 0 {
		cfg.MaxConcurrentRepairs = DefaultMaxConcurrentRepairs
	}
	return &StartRepair{base: newBase(d), cfg: cfg}
}

func (uc *StartRepair) Execute(ctx context.Context, in ports.StartRepairInput) error {
	return uc.once(ctx, in.Message, func() error { return uc.apply(ctx, in) })
}

func (uc *StartRepair) apply(ctx context.Context, in ports.StartRepairInput) error {
	log := logger.FromContext(ctx)
	exec, err := uc.repo.Get(ctx, in.Message.OSID)
	if errors.Is(err, domainerrors.ErrNotFound) {
		return uc.reject(ctx, in, nil, "execução não encontrada para a OS")
	}
	if err != nil {
		return err
	}

	switch exec.Status {
	case entities.StatusDiagnosed:
	case entities.StatusQueuedRepair, entities.StatusInRepair, entities.StatusFinished:
		// Outra mensagem (reenvio do orquestrador) já colocou o reparo em andamento.
		log.Info("command_ignored", "reason", "reparo já aceito", "status", string(exec.Status))
		return nil
	default:
		return uc.reject(ctx, in, nil, fmt.Sprintf("execução em %s não pode iniciar reparo", exec.Status))
	}

	reason, err := uc.rejection(ctx, in)
	if err != nil {
		return err
	}
	actor := entities.ServiceActor(in.Message.Producer)
	if reason != "" {
		if err := exec.RejectRepair(reason, actor, uc.now()); err != nil {
			return err
		}
		return uc.reject(ctx, in, exec, reason)
	}

	if err := exec.EnqueueRepair(in.Items, actor, uc.now()); err != nil {
		return err
	}
	if err := uc.repo.Commit(ctx, ports.Write{Execution: exec, Inbox: inbox(in.Message)}); err != nil {
		return err
	}
	log.Info("execution_status_changed", "from", string(entities.StatusDiagnosed), "to", string(exec.Status), "items", len(exec.Items))
	return nil
}

// rejection devolve o motivo da recusa ou "" quando o reparo pode ser aceito.
func (uc *StartRepair) rejection(ctx context.Context, in ports.StartRepairInput) (string, error) {
	if uc.cfg.FaultInjectionEnabled && in.SimulateFailure == contracts.FailRepair {
		return ReasonSimulatedFailure, nil
	}
	active, err := uc.repo.CountByStatus(ctx, entities.StatusQueuedRepair, entities.StatusInRepair)
	if err != nil {
		return "", err
	}
	if active >= uc.cfg.MaxConcurrentRepairs {
		return fmt.Sprintf("capacidade de reparos esgotada (%d de %d em andamento)", active, uc.cfg.MaxConcurrentRepairs), nil
	}
	return "", nil
}

// reject grava RepairRejected no outbox; exec é nil quando não há estado a mudar.
func (uc *StartRepair) reject(ctx context.Context, in ports.StartRepairInput, exec *entities.Execution, reason string) error {
	msg, err := uc.message(contracts.EvtRepairRejected, replyMeta(in.Message), contracts.RepairRejected{Reason: reason})
	if err != nil {
		return err
	}
	if err := uc.repo.Commit(ctx, ports.Write{Execution: exec, Outbox: []ports.OutboxMessage{msg}, Inbox: inbox(in.Message)}); err != nil {
		return err
	}
	logger.FromContext(ctx).Warn("repair_rejected", "reason", reason, "event_id", msg.Envelope.MessageID)
	return nil
}
