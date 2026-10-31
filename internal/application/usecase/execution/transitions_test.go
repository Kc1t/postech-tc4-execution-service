package executionuc

import (
	"context"
	"errors"
	"testing"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"go.uber.org/mock/gomock"
)

func transitionInput() ports.TransitionInput {
	return ports.TransitionInput{OSID: "os-1", Actor: mechanic, CorrelationID: "req-9"}
}

func assertEvent(t *testing.T, w ports.Write, msgType string) {
	t.Helper()
	if len(w.Outbox) != 1 {
		t.Fatalf("esperava 1 evento no outbox, veio %d", len(w.Outbox))
	}
	env := w.Outbox[0].Envelope
	if env.Type != msgType || env.SagaID != "saga-1" || env.OSID != "os-1" || env.CorrelationID != "req-9" || env.Producer != Producer {
		t.Fatalf("envelope errado: %+v", env)
	}
	if w.Outbox[0].Destination != eventsDest {
		t.Fatalf("destino errado: %+v", w.Outbox[0].Destination)
	}
	if w.Inbox != nil {
		t.Fatal("ação HTTP não grava inbox")
	}
}

func TestBeginDiagnosis(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusQueuedDiagnosis), nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	exec, err := NewBeginDiagnosis(testDeps(repo)).Execute(context.Background(), transitionInput())
	if err != nil {
		t.Fatal(err)
	}
	if exec.Status != entities.StatusInDiagnosis {
		t.Fatalf("status = %s", exec.Status)
	}
	assertEvent(t, w, contracts.EvtDiagnosisStarted)
	if got := decode[contracts.DiagnosisStarted](t, w.Outbox[0]); !got.StartedAt.Equal(fixedNow) {
		t.Fatalf("started_at = %v", got.StartedAt)
	}
	ev := exec.PendingEvents()
	if len(ev) != 1 || ev[0].Actor != mechanic {
		t.Fatalf("a linha do tempo registra o mecânico: %+v", ev)
	}
}

func TestCompleteDiagnosis(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusInDiagnosis), nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	in := ports.CompleteDiagnosisInput{
		TransitionInput: transitionInput(),
		Diagnosis: entities.Diagnosis{
			Services: []entities.DiagnosedService{{Code: 10}, {Code: 11}},
			Parts:    []entities.DiagnosedPart{{ManufacturerCode: "BOSCH-123", Quantity: 2}},
			Notes:    "pastilhas no fim",
		},
	}
	exec, err := NewCompleteDiagnosis(testDeps(repo)).Execute(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if exec.Status != entities.StatusDiagnosed || exec.Diagnosis == nil {
		t.Fatalf("execução errada: %+v", exec)
	}
	assertEvent(t, w, contracts.EvtDiagnosisCompleted)
	got := decode[contracts.DiagnosisCompleted](t, w.Outbox[0])
	if len(got.Services) != 2 || got.Services[1].Code != 11 || len(got.Parts) != 1 ||
		got.Parts[0].ManufacturerCode != "BOSCH-123" || got.Parts[0].Quantity != 2 || got.Notes != "pastilhas no fim" {
		t.Fatalf("payload errado: %+v", got)
	}
}

func TestCompleteDiagnosis_Invalido(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusInDiagnosis), nil)

	_, err := NewCompleteDiagnosis(testDeps(repo)).Execute(context.Background(), ports.CompleteDiagnosisInput{TransitionInput: transitionInput()})
	if !errors.Is(err, domainerrors.ErrInvalidDiagnosis) {
		t.Fatalf("esperava ErrInvalidDiagnosis, veio %v", err)
	}
}

func TestBeginRepair(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusQueuedRepair), nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	exec, err := NewBeginRepair(testDeps(repo)).Execute(context.Background(), transitionInput())
	if err != nil || exec.Status != entities.StatusInRepair {
		t.Fatalf("err=%v exec=%+v", err, exec)
	}
	assertEvent(t, w, contracts.EvtRepairStarted)
	if got := decode[contracts.RepairStarted](t, w.Outbox[0]); !got.StartedAt.Equal(fixedNow) {
		t.Fatalf("started_at = %v", got.StartedAt)
	}
}

func TestFinishRepair(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusInRepair), nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	exec, err := NewFinishRepair(testDeps(repo)).Execute(context.Background(), transitionInput())
	if err != nil || exec.Status != entities.StatusFinished {
		t.Fatalf("err=%v exec=%+v", err, exec)
	}
	assertEvent(t, w, contracts.EvtRepairFinished)
	if got := decode[contracts.RepairFinished](t, w.Outbox[0]); !got.FinishedAt.Equal(fixedNow) {
		t.Fatalf("finished_at = %v", got.FinishedAt)
	}
}

func TestTransicoes_Erros(t *testing.T) {
	type uc func(ports.ExecutionRepository) func(context.Context, ports.TransitionInput) (*entities.Execution, error)
	ops := map[string]struct {
		build uc
		from  entities.Status // status em que a operação é válida
	}{
		"BeginDiagnosis": {func(r ports.ExecutionRepository) func(context.Context, ports.TransitionInput) (*entities.Execution, error) {
			return NewBeginDiagnosis(testDeps(r)).Execute
		}, entities.StatusQueuedDiagnosis},
		"BeginRepair": {func(r ports.ExecutionRepository) func(context.Context, ports.TransitionInput) (*entities.Execution, error) {
			return NewBeginRepair(testDeps(r)).Execute
		}, entities.StatusQueuedRepair},
		"FinishRepair": {func(r ports.ExecutionRepository) func(context.Context, ports.TransitionInput) (*entities.Execution, error) {
			return NewFinishRepair(testDeps(r)).Execute
		}, entities.StatusInRepair},
	}
	for name, op := range ops {
		t.Run(name+"/inexistente", func(t *testing.T) {
			repo := newRepo(t)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound)
			if _, err := op.build(repo)(context.Background(), transitionInput()); !errors.Is(err, domainerrors.ErrNotFound) {
				t.Fatalf("esperava ErrNotFound, veio %v", err)
			}
		})
		t.Run(name+"/transição inválida", func(t *testing.T) {
			repo := newRepo(t)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusFinished), nil)
			if _, err := op.build(repo)(context.Background(), transitionInput()); !errors.Is(err, domainerrors.ErrInvalidTransition) {
				t.Fatalf("esperava ErrInvalidTransition, veio %v", err)
			}
		})
		t.Run(name+"/conflito de versão", func(t *testing.T) {
			repo := newRepo(t)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, op.from), nil)
			repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(domainerrors.ErrConcurrentUpdate)
			if _, err := op.build(repo)(context.Background(), transitionInput()); !errors.Is(err, domainerrors.ErrConcurrentUpdate) {
				t.Fatalf("esperava ErrConcurrentUpdate, veio %v", err)
			}
		})
	}
}
