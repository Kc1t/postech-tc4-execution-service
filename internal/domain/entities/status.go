package entities

import (
	"fmt"

	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
)

// Status é a etapa da execução na oficina.
//
//	queued_diagnosis → in_diagnosis → diagnosed → queued_repair → in_repair → finished
//	                                      └──────→ rejected
//	(qualquer status não final) ──────────────────→ cancelled
type Status string

const (
	StatusQueuedDiagnosis Status = "queued_diagnosis"
	StatusInDiagnosis     Status = "in_diagnosis"
	StatusDiagnosed       Status = "diagnosed"
	StatusQueuedRepair    Status = "queued_repair"
	StatusInRepair        Status = "in_repair"
	StatusFinished        Status = "finished"
	StatusRejected        Status = "rejected"
	StatusCancelled       Status = "cancelled"
)

// ActiveStatuses são os status não finais, na ordem do fluxo.
var ActiveStatuses = []Status{
	StatusQueuedDiagnosis, StatusInDiagnosis, StatusDiagnosed, StatusQueuedRepair, StatusInRepair,
}

// Prioridades da fila de trabalho: menor número é atendido antes. Reparo vem antes de
// diagnóstico porque o veículo já tem orçamento aprovado e pago.
const (
	PriorityRepair    = 1
	PriorityDiagnosis = 2
)

// ParseStatus converte e valida um status recebido de fora (query string, banco).
func ParseStatus(s string) (Status, error) {
	st := Status(s)
	if !st.Valid() {
		return "", fmt.Errorf("%w: %q", domainerrors.ErrInvalidStatus, s)
	}
	return st, nil
}

// Valid diz se o status é conhecido.
func (s Status) Valid() bool {
	switch s {
	case StatusQueuedDiagnosis, StatusInDiagnosis, StatusDiagnosed, StatusQueuedRepair,
		StatusInRepair, StatusFinished, StatusRejected, StatusCancelled:
		return true
	}
	return false
}

// IsFinal diz se a execução terminou (não aceita mais transições).
func (s Status) IsFinal() bool {
	return s == StatusFinished || s == StatusRejected || s == StatusCancelled
}

// IsActive diz se a execução ainda está no fluxo da oficina.
func (s Status) IsActive() bool { return s.Valid() && !s.IsFinal() }

// Priority devolve a prioridade da raia do status na fila de trabalho.
func (s Status) Priority() int {
	if s == StatusQueuedRepair || s == StatusInRepair {
		return PriorityRepair
	}
	return PriorityDiagnosis
}
