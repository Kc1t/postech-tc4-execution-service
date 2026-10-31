// Package executionuc implementa os casos de uso do execution-service: os comandos da saga
// (fila execution-commands) e as ações do mecânico (API HTTP).
//
// Toda mudança de estado vai para o repositório numa única unidade de trabalho (ports.Write):
// estado com optimistic lock, linha do tempo, outbox e, quando a mudança vem de mensagem,
// o registro de inbox. Os eventos nunca são publicados direto daqui: o relay do outbox publica
// depois que a transação foi confirmada.
package executionuc

import (
	"context"
	"errors"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
)

const (
	// Producer identifica este serviço no envelope das mensagens publicadas.
	Producer = "execution-service"
	// Consumer identifica o consumidor da fila de comandos no inbox.
	Consumer = "execution-commands"

	// conflictAttempts é quantas vezes um comando é reaplicado (relendo o estado) quando outra
	// operação grava a mesma execução no meio do caminho, antes de devolver o erro para a fila.
	conflictAttempts = 3
)

// Deps são as dependências comuns dos casos de uso.
type Deps struct {
	Repo ports.ExecutionRepository
	// Events é o tópico execution-events, destino de todos os eventos do serviço.
	Events messaging.Destination
	// Now é o relógio; nil usa time.Now.
	Now func() time.Time
}

type base struct {
	repo   ports.ExecutionRepository
	events messaging.Destination
	now    func() time.Time
}

func newBase(d Deps) base {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	return base{repo: d.Repo, events: d.Events, now: func() time.Time { return now().UTC() }}
}

// message monta um evento endereçado ao tópico do serviço.
func (b base) message(msgType string, meta contracts.Meta, payload any) (ports.OutboxMessage, error) {
	env, err := contracts.NewEnvelope(msgType, Producer, meta, payload)
	if err != nil {
		return ports.OutboxMessage{}, err
	}
	return ports.OutboxMessage{Destination: b.events, Envelope: env}, nil
}

// once aplica fn uma única vez por mensagem (padrão Idempotent Consumer).
//
// O inbox é consultado antes, para não reaplicar regra de negócio a uma reentrega (um
// StartRepair repetido não pode virar RepairRejected só porque a execução já saiu de
// diagnosed), e gravado de novo dentro da transação de fn com condição de inexistência, o que
// cobre duas entregas simultâneas. Conflito de versão relê o estado e tenta de novo.
func (b base) once(ctx context.Context, m ports.MessageMeta, fn func() error) error {
	log := logger.FromContext(ctx)
	for attempt := 1; ; attempt++ {
		done, err := b.repo.IsProcessed(ctx, Consumer, m.MessageID)
		if err != nil {
			return err
		}
		if done {
			log.Info("message_duplicate", "reason", "mensagem já processada (inbox)")
			return nil
		}
		err = fn()
		switch {
		case errors.Is(err, domainerrors.ErrDuplicateMessage):
			log.Info("message_duplicate", "reason", "entrega concorrente já gravou o inbox")
			return nil
		case errors.Is(err, domainerrors.ErrConcurrentUpdate) && attempt < conflictAttempts:
			log.Warn("command_conflict_retry", "attempt", attempt)
			continue
		}
		return err
	}
}

// inbox é o registro de idempotência da mensagem consumida.
func inbox(m ports.MessageMeta) *ports.InboxEntry {
	return &ports.InboxEntry{Consumer: Consumer, MessageID: m.MessageID, Type: m.Type}
}

// replyMeta devolve a identificação de um evento causado pela mensagem recebida.
func replyMeta(m ports.MessageMeta) contracts.Meta {
	return contracts.Meta{SagaID: m.SagaID, OSID: m.OSID, CorrelationID: m.CorrelationID, CausationID: m.MessageID}
}

// transition aplica uma ação do mecânico e grava o novo estado com o evento correspondente.
// apply muda o agregado e devolve o payload do evento.
func (b base) transition(
	ctx context.Context,
	in ports.TransitionInput,
	msgType string,
	apply func(e *entities.Execution, now time.Time) (any, error),
) (*entities.Execution, error) {
	exec, err := b.repo.Get(ctx, in.OSID)
	if err != nil {
		return nil, err
	}
	from := exec.Status
	payload, err := apply(exec, b.now())
	if err != nil {
		return nil, err
	}
	msg, err := b.message(msgType, contracts.Meta{SagaID: exec.SagaID, OSID: exec.OSID, CorrelationID: in.CorrelationID}, payload)
	if err != nil {
		return nil, err
	}
	if err := b.repo.Commit(ctx, ports.Write{Execution: exec, Outbox: []ports.OutboxMessage{msg}}); err != nil {
		return nil, err
	}
	logger.FromContext(ctx).Info("execution_status_changed",
		"os_id", exec.OSID, "saga_id", exec.SagaID,
		"from", string(from), "to", string(exec.Status),
		"event_type", msgType, "event_id", msg.Envelope.MessageID,
		"actor_id", in.Actor.ID, "actor_type", in.Actor.Type,
	)
	return exec, nil
}
