// Package entities contém o agregado Execution: a passagem de um veículo pela oficina, do
// diagnóstico ao fim do reparo, com a máquina de estados e a linha do tempo de cada mudança.
package entities

import (
	"fmt"
	"slices"
	"strings"
	"time"

	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/google/uuid"
)

// Vehicle identifica o veículo em execução.
type Vehicle struct {
	Plate string
	Brand string
	Model string
	Year  int
}

// RepairItem é um item (serviço ou peça) aprovado no orçamento e executado no reparo.
type RepairItem struct {
	Kind        string
	Ref         string
	Description string
	Quantity    int
	UnitPrice   float64
}

// Execution é o agregado raiz. A identidade é o os_id: uma execução por ordem de serviço.
//
// Version é a versão persistida (0 = ainda não gravada) e sustenta o optimistic lock: o
// repositório só grava se a versão no banco ainda for Version.
type Execution struct {
	OSID       string
	OSCode     int
	SagaID     string
	Vehicle    Vehicle
	Notes      string
	Status     Status
	Diagnosis  *Diagnosis
	Items      []RepairItem
	Reason     string
	EnqueuedAt time.Time

	CreatedAt          time.Time
	UpdatedAt          time.Time
	DiagnosisStartedAt *time.Time
	DiagnosedAt        *time.Time
	RepairStartedAt    *time.Time
	FinishedAt         *time.Time
	CancelledAt        *time.Time

	Version int

	pending []TimelineEvent
}

// NewExecutionParams são os dados recebidos no comando StartDiagnosis.
type NewExecutionParams struct {
	OSID    string
	OSCode  int
	SagaID  string
	Vehicle Vehicle
	Notes   string
}

// NewExecution cria a execução já na fila de diagnóstico.
func NewExecution(p NewExecutionParams, actor Actor, now time.Time) (*Execution, error) {
	if strings.TrimSpace(p.OSID) == "" {
		return nil, fmt.Errorf("%w: os_id obrigatório", domainerrors.ErrInvalidExecution)
	}
	now = now.UTC()
	e := &Execution{
		OSID:       p.OSID,
		OSCode:     p.OSCode,
		SagaID:     p.SagaID,
		Vehicle:    p.Vehicle,
		Notes:      p.Notes,
		Status:     StatusQueuedDiagnosis,
		EnqueuedAt: now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	e.record(EventDiagnosisQueued, "", StatusQueuedDiagnosis, actor, now, nil)
	return e, nil
}

// NewCancelledExecution cria uma execução já cancelada. É usada quando o cancelamento chega
// antes do StartDiagnosis (a fila padrão do SQS não garante ordem): o registro impede que um
// StartDiagnosis atrasado abra uma execução para uma saga que já foi compensada.
func NewCancelledExecution(osID, sagaID, reason string, actor Actor, now time.Time) (*Execution, error) {
	if strings.TrimSpace(osID) == "" {
		return nil, fmt.Errorf("%w: os_id obrigatório", domainerrors.ErrInvalidExecution)
	}
	now = now.UTC()
	e := &Execution{
		OSID:        osID,
		SagaID:      sagaID,
		Status:      StatusCancelled,
		Reason:      reason,
		EnqueuedAt:  now,
		CreatedAt:   now,
		UpdatedAt:   now,
		CancelledAt: &now,
	}
	e.record(EventCancelled, "", StatusCancelled, actor, now, reasonDetails(reason))
	return e, nil
}

// IsNew diz se a execução ainda não foi gravada.
func (e *Execution) IsNew() bool { return e.Version == 0 }

// Priority é a prioridade da execução na fila de trabalho.
func (e *Execution) Priority() int { return e.Status.Priority() }

// BeginDiagnosis: o mecânico pega o veículo da fila de diagnóstico.
func (e *Execution) BeginDiagnosis(actor Actor, now time.Time) error {
	now = now.UTC()
	if err := e.transition(StatusInDiagnosis, EventDiagnosisStarted, actor, now, nil, StatusQueuedDiagnosis); err != nil {
		return err
	}
	e.DiagnosisStartedAt = &now
	return nil
}

// CompleteDiagnosis registra os serviços e peças necessários.
func (e *Execution) CompleteDiagnosis(d Diagnosis, actor Actor, now time.Time) error {
	if err := d.Validate(); err != nil {
		return err
	}
	now = now.UTC()
	details := map[string]string{
		"services": fmt.Sprint(len(d.Services)),
		"parts":    fmt.Sprint(len(d.Parts)),
	}
	if err := e.transition(StatusDiagnosed, EventDiagnosisCompleted, actor, now, details, StatusInDiagnosis); err != nil {
		return err
	}
	d.CompletedAt = now
	e.Diagnosis = &d
	e.DiagnosedAt = &now
	return nil
}

// EnqueueRepair coloca a execução na fila de reparo com os itens aprovados no orçamento.
func (e *Execution) EnqueueRepair(items []RepairItem, actor Actor, now time.Time) error {
	now = now.UTC()
	details := map[string]string{"items": fmt.Sprint(len(items))}
	if err := e.transition(StatusQueuedRepair, EventRepairQueued, actor, now, details, StatusDiagnosed); err != nil {
		return err
	}
	e.Items = slices.Clone(items)
	e.EnqueuedAt = now
	return nil
}

// RejectRepair recusa o reparo (capacidade esgotada ou falha simulada).
func (e *Execution) RejectRepair(reason string, actor Actor, now time.Time) error {
	now = now.UTC()
	if err := e.transition(StatusRejected, EventRepairRejected, actor, now, reasonDetails(reason), StatusDiagnosed); err != nil {
		return err
	}
	e.Reason = reason
	return nil
}

// BeginRepair: o mecânico pega o veículo da fila de reparo.
func (e *Execution) BeginRepair(actor Actor, now time.Time) error {
	now = now.UTC()
	if err := e.transition(StatusInRepair, EventRepairStarted, actor, now, nil, StatusQueuedRepair); err != nil {
		return err
	}
	e.RepairStartedAt = &now
	return nil
}

// FinishRepair conclui o reparo.
func (e *Execution) FinishRepair(actor Actor, now time.Time) error {
	now = now.UTC()
	if err := e.transition(StatusFinished, EventRepairFinished, actor, now, nil, StatusInRepair); err != nil {
		return err
	}
	e.FinishedAt = &now
	return nil
}

// Cancel encerra a execução em qualquer status não final (compensação da saga).
func (e *Execution) Cancel(reason string, actor Actor, now time.Time) error {
	now = now.UTC()
	if err := e.transition(StatusCancelled, EventCancelled, actor, now, reasonDetails(reason), ActiveStatuses...); err != nil {
		return err
	}
	e.Reason = reason
	e.CancelledAt = &now
	return nil
}

// PendingEvents devolve os eventos de linha do tempo ainda não gravados.
func (e *Execution) PendingEvents() []TimelineEvent { return slices.Clone(e.pending) }

// ClearPendingEvents é chamado pelo repositório depois de gravar.
func (e *Execution) ClearPendingEvents() { e.pending = nil }

func (e *Execution) transition(to Status, evt EventType, actor Actor, now time.Time, details map[string]string, from ...Status) error {
	if !slices.Contains(from, e.Status) {
		return fmt.Errorf("%w: %s → %s", domainerrors.ErrInvalidTransition, e.Status, to)
	}
	prev := e.Status
	e.Status = to
	e.UpdatedAt = now
	e.record(evt, prev, to, actor, now, details)
	return nil
}

func (e *Execution) record(evt EventType, from, to Status, actor Actor, now time.Time, details map[string]string) {
	e.pending = append(e.pending, TimelineEvent{
		ID:         uuid.NewString(),
		Type:       evt,
		From:       from,
		To:         to,
		Actor:      actor,
		Details:    details,
		OccurredAt: now,
	})
}

func reasonDetails(reason string) map[string]string {
	if reason == "" {
		return nil
	}
	return map[string]string{"reason": reason}
}
