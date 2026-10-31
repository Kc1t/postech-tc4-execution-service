// Package commands traduz os comandos da fila execution-commands (contratos do pkg/contracts)
// para as entradas dos casos de uso. É a camada anticorrupção entre o contrato publicado e o
// domínio.
//
// Retornar erro deixa a mensagem na fila para nova tentativa (e, esgotado o maxReceiveCount,
// DLQ). Por isso só falhas transitórias e mensagens malformadas viram erro; regra de negócio
// violada é tratada no caso de uso como evento de falha.
package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
)

// ErrMissingOSID indica um comando sem os_id no envelope (vai para a DLQ para análise).
var ErrMissingOSID = errors.New("commands: envelope sem os_id")

// Registrar é o que o handler precisa do consumidor (messaging.Consumer satisfaz).
type Registrar interface {
	Handle(msgType string, h messaging.Handler)
}

// Handler despacha cada tipo de comando para o caso de uso correspondente.
type Handler struct {
	startDiagnosis ports.StartDiagnosisUseCase
	startRepair    ports.StartRepairUseCase
	cancel         ports.CancelExecutionUseCase
}

func NewHandler(sd ports.StartDiagnosisUseCase, sr ports.StartRepairUseCase, ce ports.CancelExecutionUseCase) *Handler {
	return &Handler{startDiagnosis: sd, startRepair: sr, cancel: ce}
}

// Register associa os tipos de comando aos métodos do handler.
func (h *Handler) Register(r Registrar) {
	r.Handle(contracts.CmdStartDiagnosis, h.StartDiagnosis)
	r.Handle(contracts.CmdStartRepair, h.StartRepair)
	r.Handle(contracts.CmdCancelExecution, h.CancelExecution)
}

func (h *Handler) StartDiagnosis(ctx context.Context, env contracts.Envelope) error {
	p, meta, err := decode[contracts.StartDiagnosis](env)
	if err != nil {
		return err
	}
	return h.startDiagnosis.Execute(ctx, ports.StartDiagnosisInput{
		Message: meta,
		OSCode:  p.OSCode,
		Vehicle: entities.Vehicle{Plate: p.Vehicle.Plate, Brand: p.Vehicle.Brand, Model: p.Vehicle.Model, Year: p.Vehicle.Year},
		Notes:   p.Notes,
	})
}

func (h *Handler) StartRepair(ctx context.Context, env contracts.Envelope) error {
	p, meta, err := decode[contracts.StartRepair](env)
	if err != nil {
		return err
	}
	items := make([]entities.RepairItem, 0, len(p.Items))
	for _, i := range p.Items {
		items = append(items, entities.RepairItem{
			Kind: i.Kind, Ref: i.Ref, Description: i.Description, Quantity: i.Quantity, UnitPrice: i.UnitPrice,
		})
	}
	return h.startRepair.Execute(ctx, ports.StartRepairInput{Message: meta, Items: items, SimulateFailure: p.SimulateFailure})
}

func (h *Handler) CancelExecution(ctx context.Context, env contracts.Envelope) error {
	p, meta, err := decode[contracts.CancelExecution](env)
	if err != nil {
		return err
	}
	return h.cancel.Execute(ctx, ports.CancelExecutionInput{Message: meta, Reason: p.Reason})
}

func decode[T any](env contracts.Envelope) (T, ports.MessageMeta, error) {
	var zero T
	if env.OSID == "" {
		return zero, ports.MessageMeta{}, fmt.Errorf("%w: %s %s", ErrMissingOSID, env.Type, env.MessageID)
	}
	p, err := contracts.Decode[T](env)
	if err != nil {
		return zero, ports.MessageMeta{}, err
	}
	return p, ports.MessageMeta{
		MessageID:     env.MessageID,
		Type:          env.Type,
		SagaID:        env.SagaID,
		OSID:          env.OSID,
		CorrelationID: env.CorrelationID,
		Producer:      env.Producer,
	}, nil
}
