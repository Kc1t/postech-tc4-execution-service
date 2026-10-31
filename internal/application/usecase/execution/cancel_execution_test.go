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

func cancelInput(reason string) ports.CancelExecutionInput {
	return ports.CancelExecutionInput{Message: msgMeta(contracts.CmdCancelExecution), Reason: reason}
}

func assertCancelled(t *testing.T, w ports.Write, reason string) {
	t.Helper()
	if w.Execution == nil || w.Execution.Status != entities.StatusCancelled || w.Execution.Reason != reason {
		t.Fatalf("execução não cancelada: %+v", w.Execution)
	}
	if len(w.Outbox) != 1 || w.Outbox[0].Envelope.Type != contracts.EvtExecutionCancelled || w.Outbox[0].Envelope.CausationID != "msg-1" {
		t.Fatalf("esperava ExecutionCancelled no outbox: %+v", w.Outbox)
	}
	if got := decode[contracts.ExecutionCancelled](t, w.Outbox[0]); got.Reason != reason {
		t.Fatalf("motivo = %q", got.Reason)
	}
	if w.Inbox == nil || w.Inbox.Type != contracts.CmdCancelExecution {
		t.Fatalf("inbox errado: %+v", w.Inbox)
	}
}

func TestCancelExecution_StatusAtivos(t *testing.T) {
	for _, s := range entities.ActiveStatuses {
		t.Run(string(s), func(t *testing.T) {
			repo := newRepo(t)
			var w ports.Write
			repo.EXPECT().IsProcessed(gomock.Any(), Consumer, "msg-1").Return(false, nil)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, s), nil)
			repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

			if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("pagamento recusado")); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			assertCancelled(t, w, "pagamento recusado")
			if w.Execution.IsNew() {
				t.Fatal("cancelamento de execução existente grava com condição de versão")
			}
		})
	}
}

func TestCancelExecution_MotivoPadrao(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusQueuedDiagnosis), nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("")); err != nil {
		t.Fatal(err)
	}
	assertCancelled(t, w, DefaultCancelReason)
}

func TestCancelExecution_AntesDoStartDiagnosisGravaExecucaoCancelada(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("saga compensada")); err != nil {
		t.Fatal(err)
	}
	assertCancelled(t, w, "saga compensada")
	if !w.Execution.IsNew() || w.Execution.SagaID != "saga-1" {
		t.Fatalf("esperava execução nova e cancelada: %+v", w.Execution)
	}
}

func TestCancelExecution_StatusFinalIgnorado(t *testing.T) {
	for _, s := range []entities.Status{entities.StatusFinished, entities.StatusRejected, entities.StatusCancelled} {
		t.Run(string(s), func(t *testing.T) {
			repo := newRepo(t)
			repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, s), nil)
			if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("x")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCancelExecution_CorridaComStartDiagnosisReaplica(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	gomock.InOrder(
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil),
		repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound),
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(domainerrors.ErrAlreadyExists),
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil),
		repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusQueuedDiagnosis), nil),
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil)),
	)
	if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("x")); err != nil {
		t.Fatal(err)
	}
	assertCancelled(t, w, "x")
}

func TestCancelExecution_Erros(t *testing.T) {
	boom := errors.New("boom")
	t.Run("leitura", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, boom)
		if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("x")); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("gravação", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(execAt(t, entities.StatusInRepair), nil)
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(boom)
		if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), cancelInput("x")); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("sem os_id", func(t *testing.T) {
		repo := newRepo(t)
		in := cancelInput("x")
		in.Message.OSID = ""
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), "").Return(nil, domainerrors.ErrNotFound)
		if err := NewCancelExecution(testDeps(repo)).Execute(context.Background(), in); !errors.Is(err, domainerrors.ErrInvalidExecution) {
			t.Fatalf("esperava ErrInvalidExecution, veio %v", err)
		}
	})
}
