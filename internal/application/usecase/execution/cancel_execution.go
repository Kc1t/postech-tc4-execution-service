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

// DefaultCancelReason é usado quando o comando chega sem motivo.
const DefaultCancelReason = "cancelada pela saga"

// CancelExecution trata o comando execution.CancelExecution (compensação): qualquer status não
// final vai para cancelled e publica ExecutionCancelled. Status final é ignorado.
//
// Se a execução ainda não existe (o cancelamento passou na frente do StartDiagnosis na fila),
// grava uma execução já cancelada: o StartDiagnosis atrasado encontra o registro e não reabre.
type CancelExecution struct{ base }

func NewCancelExecution(d Deps) *CancelExecution { return &CancelExecution{newBase(d)} }

func (uc *CancelExecution) Execute(ctx context.Context, in ports.CancelExecutionInput) error {
	return uc.once(ctx, in.Message, func() error { return uc.apply(ctx, in) })
}

func (uc *CancelExecution) apply(ctx context.Context, in ports.CancelExecutionInput) error {
	log := logger.FromContext(ctx)
	reason := in.Reason
	if reason == "" {
		reason = DefaultCancelReason
	}
	actor := entities.ServiceActor(in.Message.Producer)

	exec, err := uc.repo.Get(ctx, in.Message.OSID)
	from := entities.Status("")
	switch {
	case errors.Is(err, domainerrors.ErrNotFound):
		exec, err = entities.NewCancelledExecution(in.Message.OSID, in.Message.SagaID, reason, actor, uc.now())
		if err != nil {
			return err
		}
	case err != nil:
		return err
	case exec.Status.IsFinal():
		log.Info("command_ignored", "reason", "execução já encerrada", "status", string(exec.Status))
		return nil
	default:
		from = exec.Status
		if err := exec.Cancel(reason, actor, uc.now()); err != nil {
			return err
		}
	}

	msg, err := uc.message(contracts.EvtExecutionCancelled, replyMeta(in.Message), contracts.ExecutionCancelled{Reason: reason})
	if err != nil {
		return err
	}
	err = uc.repo.Commit(ctx, ports.Write{Execution: exec, Outbox: []ports.OutboxMessage{msg}, Inbox: inbox(in.Message)})
	if errors.Is(err, domainerrors.ErrAlreadyExists) {
		// StartDiagnosis criou a execução entre a leitura e a escrita: relê e cancela.
		return fmt.Errorf("%w: execução criada durante o cancelamento", domainerrors.ErrConcurrentUpdate)
	}
	if err != nil {
		return err
	}
	log.Info("execution_status_changed", "from", string(from), "to", string(exec.Status), "reason", reason, "event_id", msg.Envelope.MessageID)
	return nil
}
