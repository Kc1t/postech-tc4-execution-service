package commands

// Contrato do consumidor: o exemplo publicado no schema de cada comando da fila
// execution-commands, entregue ao handler registrado no consumidor (com os casos de uso reais e o
// repositório simulado), é aceito e produz o efeito do contrato.

import (
	"context"
	"testing"
	"time"

	executionuc "github.com/Kc1t/postech-tc4-execution-service/internal/application/usecase/execution"
	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports/mocks"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"go.uber.org/mock/gomock"
)

func example(t *testing.T, msgType string) contracts.Envelope {
	t.Helper()
	examples, err := contracts.Examples(msgType)
	if err != nil {
		t.Fatal(err)
	}
	env, err := contracts.Parse(examples[0])
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func examplePayload[T any](t *testing.T, msgType string) T {
	t.Helper()
	v, err := contracts.Decode[T](example(t, msgType))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// diagnosedExecution é a execução da OS do exemplo, já diagnosticada.
func diagnosedExecution(t *testing.T, env contracts.Envelope) *entities.Execution {
	t.Helper()
	at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	saga := entities.ServiceActor("os-service")
	mech := entities.Actor{ID: "mec-1", Type: entities.ActorMechanic}
	e, err := entities.NewExecution(entities.NewExecutionParams{OSID: env.OSID, OSCode: 1042, SagaID: env.SagaID,
		Vehicle: entities.Vehicle{Plate: "BRA2E19"}}, saga, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.BeginDiagnosis(mech, at); err != nil {
		t.Fatal(err)
	}
	if err := e.CompleteDiagnosis(entities.Diagnosis{Services: []entities.DiagnosedService{{Code: 4012}}}, mech, at); err != nil {
		t.Fatal(err)
	}
	e.Version = 2
	e.ClearPendingEvents()
	return e
}

func TestContrato_ConsumidorAceitaOsExemplosDosComandos(t *testing.T) {
	ctx := context.Background()
	repo := mocks.NewMockExecutionRepository(gomock.NewController(t))
	deps := executionuc.Deps{
		Repo:   repo,
		Events: messaging.Destination{Kind: messaging.Topic, Name: "arn:aws:sns:us-east-1:000000000000:execution-events"},
	}
	reg := registry{}
	NewHandler(
		executionuc.NewStartDiagnosis(deps),
		executionuc.NewStartRepair(deps, executionuc.StartRepairConfig{}),
		executionuc.NewCancelExecution(deps),
	).Register(reg)

	// deliver entrega o exemplo do tipo ao handler registrado e devolve a gravação feita.
	deliver := func(msgType string, current *entities.Execution) (contracts.Envelope, ports.Write) {
		t.Helper()
		env := example(t, msgType)
		repo.EXPECT().IsProcessed(gomock.Any(), executionuc.Consumer, env.MessageID).Return(false, nil)
		if current != nil {
			repo.EXPECT().Get(gomock.Any(), env.OSID).Return(current, nil)
		} else {
			repo.EXPECT().Get(gomock.Any(), env.OSID).Return(nil, domainerrors.ErrNotFound)
		}
		var w ports.Write
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, got ports.Write) error {
			w = got
			return nil
		})
		h, ok := reg[msgType]
		if !ok {
			t.Fatalf("nenhum handler registrado para %s", msgType)
		}
		if err := h(ctx, env); err != nil {
			t.Fatalf("o exemplo de %s foi recusado: %v", msgType, err)
		}
		if w.Inbox == nil || w.Inbox.MessageID != env.MessageID {
			t.Fatalf("%s: o comando precisa ser registrado no inbox: %+v", msgType, w.Inbox)
		}
		return env, w
	}
	covered := map[string]bool{}

	// StartDiagnosis abre a execução na fila de diagnóstico com os dados do exemplo.
	covered[contracts.CmdStartDiagnosis] = true
	sd := examplePayload[contracts.StartDiagnosis](t, contracts.CmdStartDiagnosis)
	env, w := deliver(contracts.CmdStartDiagnosis, nil)
	e := w.Execution
	if e == nil || e.Status != entities.StatusQueuedDiagnosis || e.OSID != env.OSID || e.SagaID != env.SagaID ||
		e.OSCode != sd.OSCode || e.Vehicle.Plate != sd.Vehicle.Plate || e.Vehicle.Year != sd.Vehicle.Year || e.Notes != sd.Notes {
		t.Fatalf("execução aberta pelo exemplo: %+v", e)
	}

	// StartRepair coloca a execução diagnosticada na fila de reparo com os itens do exemplo
	// (a capacidade padrão sobra).
	covered[contracts.CmdStartRepair] = true
	sr := examplePayload[contracts.StartRepair](t, contracts.CmdStartRepair)
	repo.EXPECT().CountByStatus(gomock.Any(), entities.StatusQueuedRepair, entities.StatusInRepair).Return(0, nil)
	_, w = deliver(contracts.CmdStartRepair, diagnosedExecution(t, example(t, contracts.CmdStartRepair)))
	if e := w.Execution; e == nil || e.Status != entities.StatusQueuedRepair || len(e.Items) != len(sr.Items) ||
		e.Items[1].Ref != sr.Items[1].Ref || e.Items[1].Quantity != sr.Items[1].Quantity || len(w.Outbox) != 0 {
		t.Fatalf("reparo aceito pelo exemplo: %+v outbox=%d", e, len(w.Outbox))
	}

	// CancelExecution cancela e confirma com ExecutionCancelled, causado pelo comando.
	covered[contracts.CmdCancelExecution] = true
	ce := examplePayload[contracts.CancelExecution](t, contracts.CmdCancelExecution)
	env, w = deliver(contracts.CmdCancelExecution, diagnosedExecution(t, example(t, contracts.CmdCancelExecution)))
	if e := w.Execution; e == nil || e.Status != entities.StatusCancelled || e.Reason != ce.Reason || len(w.Outbox) != 1 {
		t.Fatalf("cancelamento pelo exemplo: %+v", e)
	}
	reply := w.Outbox[0].Envelope
	body, err := reply.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := contracts.Validate(body); err != nil {
		t.Fatalf("resposta fora do contrato: %v", err)
	}
	if reply.Type != contracts.EvtExecutionCancelled || reply.CausationID != env.MessageID || reply.CorrelationID != env.CorrelationID {
		t.Fatalf("resposta: %+v", reply)
	}

	for typ := range reg {
		if !covered[typ] {
			t.Errorf("o execution-service consome %s, mas o teste não entregou o exemplo dele", typ)
		}
	}
}
