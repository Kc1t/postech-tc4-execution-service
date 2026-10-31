package executionuc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"go.uber.org/mock/gomock"
)

func startRepairInput(fail string) ports.StartRepairInput {
	return ports.StartRepairInput{
		Message: msgMeta(contracts.CmdStartRepair),
		Items: []entities.RepairItem{
			{Kind: contracts.ItemKindService, Ref: "10", Description: "Troca de pastilhas", Quantity: 1, UnitPrice: 150},
			{Kind: contracts.ItemKindPart, Ref: "BOSCH-123", Description: "Pastilha", Quantity: 2, UnitPrice: 80},
		},
		SimulateFailure: fail,
	}
}

func newStartRepair(repo ports.ExecutionRepository, max int, fault bool) *StartRepair {
	return NewStartRepair(testDeps(repo), StartRepairConfig{MaxConcurrentRepairs: max, FaultInjectionEnabled: fault})
}

func TestStartRepair_EnfileiraReparo(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), Consumer, "msg-1").Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusDiagnosed), nil)
	repo.EXPECT().CountByStatus(gomock.Any(), entities.StatusQueuedRepair, entities.StatusInRepair).Return(9, nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	e := w.Execution
	if e.Status != entities.StatusQueuedRepair || len(e.Items) != 2 || e.Items[1].Ref != "BOSCH-123" || !e.EnqueuedAt.Equal(fixedNow) {
		t.Fatalf("execução errada: %+v", e)
	}
	if len(w.Outbox) != 0 || w.Inbox == nil || w.Inbox.Type != contracts.CmdStartRepair {
		t.Fatalf("aceite não publica evento e grava o inbox: %+v", w)
	}
}

func TestStartRepair_CapacidadeEsgotada(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusDiagnosed), nil)
	repo.EXPECT().CountByStatus(gomock.Any(), entities.StatusQueuedRepair, entities.StatusInRepair).Return(2, nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := newStartRepair(repo, 2, false).Execute(context.Background(), startRepairInput("")); err != nil {
		t.Fatalf("regra de negócio violada não é erro: %v", err)
	}
	if w.Execution == nil || w.Execution.Status != entities.StatusRejected || !strings.Contains(w.Execution.Reason, "capacidade") {
		t.Fatalf("esperava rejected por capacidade: %+v", w.Execution)
	}
	assertRejected(t, w, "capacidade de reparos esgotada (2 de 2 em andamento)")
}

func TestStartRepair_FalhaSimulada(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusDiagnosed), nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := newStartRepair(repo, 10, true).Execute(context.Background(), startRepairInput(contracts.FailRepair)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if w.Execution.Status != entities.StatusRejected {
		t.Fatalf("esperava rejected, veio %s", w.Execution.Status)
	}
	assertRejected(t, w, ReasonSimulatedFailure)
}

func TestStartRepair_FalhaSimuladaIgnoradaSemFlag(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusDiagnosed), nil)
	repo.EXPECT().CountByStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(0, nil)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput(contracts.FailRepair)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if w.Execution.Status != entities.StatusQueuedRepair {
		t.Fatalf("com FAULT_INJECTION_ENABLED=false o campo é ignorado; veio %s", w.Execution.Status)
	}
}

func TestStartRepair_SemExecucaoRecusaSemEstado(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if w.Execution != nil {
		t.Fatal("não há estado para gravar")
	}
	assertRejected(t, w, "execução não encontrada para a OS")
}

func TestStartRepair_StatusErradoRecusaSemMudarEstado(t *testing.T) {
	for _, s := range []entities.Status{entities.StatusQueuedDiagnosis, entities.StatusInDiagnosis, entities.StatusCancelled, entities.StatusRejected} {
		t.Run(string(s), func(t *testing.T) {
			repo := newRepo(t)
			var w ports.Write
			repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, s), nil)
			repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

			if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if w.Execution != nil {
				t.Fatal("o estado não muda")
			}
			assertRejected(t, w, "execução em "+string(s)+" não pode iniciar reparo")
		})
	}
}

func TestStartRepair_ReparoJaAceitoEIgnorado(t *testing.T) {
	for _, s := range []entities.Status{entities.StatusQueuedRepair, entities.StatusInRepair, entities.StatusFinished} {
		t.Run(string(s), func(t *testing.T) {
			repo := newRepo(t)
			repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, s), nil)
			if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); err != nil {
				t.Fatalf("Execute: %v", err)
			}
		})
	}
}

func TestStartRepair_Erros(t *testing.T) {
	boom := errors.New("boom")
	t.Run("leitura", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, boom)
		if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("contagem", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(execAt(t, entities.StatusDiagnosed), nil)
		repo.EXPECT().CountByStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(0, boom)
		if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("gravação do aceite", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(execAt(t, entities.StatusDiagnosed), nil)
		repo.EXPECT().CountByStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(0, nil)
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(boom)
		if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("gravação da recusa", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, domainerrors.ErrNotFound)
		repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(boom)
		if err := newStartRepair(repo, 10, false).Execute(context.Background(), startRepairInput("")); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
}

func TestNewStartRepair_CapacidadePadrao(t *testing.T) {
	if uc := NewStartRepair(Deps{}, StartRepairConfig{}); uc.cfg.MaxConcurrentRepairs != DefaultMaxConcurrentRepairs {
		t.Fatalf("capacidade padrão = %d", uc.cfg.MaxConcurrentRepairs)
	}
}

func assertRejected(t *testing.T, w ports.Write, reason string) {
	t.Helper()
	if len(w.Outbox) != 1 {
		t.Fatalf("esperava 1 mensagem no outbox, veio %d", len(w.Outbox))
	}
	m := w.Outbox[0]
	env := m.Envelope
	if m.Destination != eventsDest || env.Type != contracts.EvtRepairRejected || env.Producer != Producer {
		t.Fatalf("mensagem errada: %+v", m)
	}
	if env.SagaID != "saga-1" || env.OSID != "os-1" || env.CorrelationID != "corr-1" || env.CausationID != "msg-1" {
		t.Fatalf("meta errada: %+v", env)
	}
	if got := decode[contracts.RepairRejected](t, m); got.Reason != reason {
		t.Fatalf("motivo = %q, esperava %q", got.Reason, reason)
	}
	if w.Inbox == nil || w.Inbox.MessageID != "msg-1" {
		t.Fatalf("a recusa grava o inbox na mesma transação: %+v", w.Inbox)
	}
}
