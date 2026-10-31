package executionuc

// Contrato do produtor: todo evento que os casos de uso entregam ao outbox (ports.Write.Outbox,
// gravado pelo repositório DynamoDB como Envelope.Marshal) passa no JSON Schema do seu tipo
// (pkg/contracts/schemas).

import (
	"context"
	"slices"
	"testing"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

func TestContrato_ProdutorEventosSeguemOSchema(t *testing.T) {
	ctx := context.Background()
	osID, sagaID := uuid.NewString(), uuid.NewString()
	// Execuções com os IDs no formato que o os-service envia (os helpers dos outros testes usam
	// "os-1"/"saga-1", que não são UUID).
	at := func(s entities.Status) *entities.Execution {
		e := execAt(t, s)
		e.OSID, e.SagaID = osID, sagaID
		return e
	}
	mech := ports.TransitionInput{OSID: osID, Actor: mechanic, CorrelationID: "req-" + uuid.NewString()[:8]}
	command := func(msgType string) ports.MessageMeta {
		return ports.MessageMeta{
			MessageID: uuid.NewString(), Type: msgType, SagaID: sagaID, OSID: osID,
			CorrelationID: uuid.NewString(), Producer: "os-service",
		}
	}
	items := []entities.RepairItem{
		{Kind: contracts.ItemKindService, Ref: "4012", Description: "Troca de pastilhas", Quantity: 1, UnitPrice: 150},
		{Kind: contracts.ItemKindPart, Ref: "BOSCH-0986BB0606", Description: "Pastilha dianteira", Quantity: 2, UnitPrice: 90},
	}

	var outbox []ports.OutboxMessage
	// run executa um caso de uso com o repositório devolvendo current (nil = execução inexistente).
	run := func(current *entities.Execution, fn func(d Deps) error) {
		t.Helper()
		repo := newRepo(t)
		if current != nil {
			repo.EXPECT().Get(gomock.Any(), osID).Return(current, nil).AnyTimes()
		} else {
			repo.EXPECT().Get(gomock.Any(), osID).Return(nil, domainerrors.ErrNotFound).AnyTimes()
		}
		repo.EXPECT().IsProcessed(gomock.Any(), Consumer, gomock.Any()).Return(false, nil).AnyTimes()
		repo.EXPECT().CountByStatus(gomock.Any(), entities.StatusQueuedRepair, entities.StatusInRepair).Return(0, nil).AnyTimes()
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, w ports.Write) error {
			outbox = append(outbox, w.Outbox...)
			return nil
		})
		if err := fn(testDeps(repo)); err != nil {
			t.Fatal(err)
		}
	}

	// Ações do mecânico (API).
	run(at(entities.StatusQueuedDiagnosis), func(d Deps) error {
		_, err := NewBeginDiagnosis(d).Execute(ctx, mech)
		return err
	})
	run(at(entities.StatusInDiagnosis), func(d Deps) error {
		_, err := NewCompleteDiagnosis(d).Execute(ctx, ports.CompleteDiagnosisInput{TransitionInput: mech, Diagnosis: entities.Diagnosis{
			Services: []entities.DiagnosedService{{Code: 4012}},
			Parts:    []entities.DiagnosedPart{{ManufacturerCode: "BOSCH-0986BB0606", Quantity: 2}},
			Notes:    "pastilhas no limite",
		}})
		return err
	})
	run(at(entities.StatusInDiagnosis), func(d Deps) error { // diagnóstico só com serviço: parts vira []
		_, err := NewCompleteDiagnosis(d).Execute(ctx, ports.CompleteDiagnosisInput{TransitionInput: mech, Diagnosis: entities.Diagnosis{
			Services: []entities.DiagnosedService{{Code: 4012}},
		}})
		return err
	})
	run(at(entities.StatusQueuedRepair), func(d Deps) error {
		_, err := NewBeginRepair(d).Execute(ctx, mech)
		return err
	})
	run(at(entities.StatusInRepair), func(d Deps) error {
		_, err := NewFinishRepair(d).Execute(ctx, mech)
		return err
	})

	// Respostas aos comandos da saga.
	faulty := StartRepairConfig{FaultInjectionEnabled: true}
	run(at(entities.StatusDiagnosed), func(d Deps) error {
		return NewStartRepair(d, faulty).Execute(ctx, ports.StartRepairInput{
			Message: command(contracts.CmdStartRepair), Items: items, SimulateFailure: contracts.FailRepair,
		})
	})
	run(nil, func(d Deps) error { // StartRepair de OS desconhecida
		return NewStartRepair(d, faulty).Execute(ctx, ports.StartRepairInput{Message: command(contracts.CmdStartRepair), Items: items})
	})
	run(at(entities.StatusInDiagnosis), func(d Deps) error {
		return NewCancelExecution(d).Execute(ctx, ports.CancelExecutionInput{Message: command(contracts.CmdCancelExecution), Reason: "cliente desistiu"})
	})
	run(nil, func(d Deps) error { // cancelamento antes do StartDiagnosis, sem motivo
		return NewCancelExecution(d).Execute(ctx, ports.CancelExecutionInput{Message: command(contracts.CmdCancelExecution)})
	})

	seen := map[string]int{}
	for _, m := range outbox {
		body, err := m.Envelope.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if err := contracts.Validate(body); err != nil {
			t.Errorf("%s fora do contrato: %v\n%s", m.Envelope.Type, err, body)
		}
		if m.Destination != eventsDest {
			t.Errorf("%s deveria ir para o tópico execution-events, foi para %+v", m.Envelope.Type, m.Destination)
		}
		seen[m.Envelope.Type]++
	}
	want := []string{
		contracts.EvtDiagnosisStarted, contracts.EvtDiagnosisCompleted, contracts.EvtRepairStarted,
		contracts.EvtRepairFinished, contracts.EvtRepairRejected, contracts.EvtExecutionCancelled,
	}
	for _, typ := range want {
		if seen[typ] == 0 {
			t.Errorf("nenhum %s produzido: o teste precisa cobrir todos os eventos do execution-service", typ)
		}
	}
	for typ := range seen {
		if !slices.Contains(want, typ) {
			t.Errorf("o execution-service produziu %s, fora da lista de eventos que ele publica", typ)
		}
	}
	t.Logf("eventos validados por tipo: %v", seen)
}
