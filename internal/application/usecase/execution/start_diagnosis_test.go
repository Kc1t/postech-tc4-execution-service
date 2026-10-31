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

func startDiagnosisInput() ports.StartDiagnosisInput {
	return ports.StartDiagnosisInput{
		Message: msgMeta(contracts.CmdStartDiagnosis),
		OSCode:  42,
		Vehicle: entities.Vehicle{Plate: "ABC1D23", Brand: "Fiat", Model: "Uno", Year: 2020},
		Notes:   "barulho no freio",
	}
}

func TestStartDiagnosis_CriaExecucao(t *testing.T) {
	repo := newRepo(t)
	var w ports.Write
	repo.EXPECT().IsProcessed(gomock.Any(), Consumer, "msg-1").Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(capture(&w, nil))

	if err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	e := w.Execution
	if e == nil || e.Status != entities.StatusQueuedDiagnosis || e.OSCode != 42 || e.SagaID != "saga-1" ||
		e.Vehicle.Plate != "ABC1D23" || e.Notes != "barulho no freio" || !e.EnqueuedAt.Equal(fixedNow) {
		t.Fatalf("execução gravada errada: %+v", e)
	}
	if len(w.Outbox) != 0 {
		t.Fatal("StartDiagnosis não publica evento")
	}
	if w.Inbox == nil || w.Inbox.Consumer != Consumer || w.Inbox.MessageID != "msg-1" || w.Inbox.Type != contracts.CmdStartDiagnosis {
		t.Fatalf("inbox errado: %+v", w.Inbox)
	}
	ev := e.PendingEvents()
	if len(ev) != 1 || ev[0].Actor.ID != "os-service" || ev[0].Actor.Type != entities.ActorService {
		t.Fatalf("linha do tempo errada: %+v", ev)
	}
}

func TestStartDiagnosis_IdempotentePorOSID(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().IsProcessed(gomock.Any(), Consumer, "msg-1").Return(false, nil)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(execAt(t, entities.StatusInDiagnosis), nil)

	if err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

func TestStartDiagnosis_MensagemDuplicada(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().IsProcessed(gomock.Any(), Consumer, "msg-1").Return(true, nil)

	if err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}

func TestStartDiagnosis_ConflitosNoCommit(t *testing.T) {
	cases := map[string]struct {
		commitErr error
		wantErr   bool
	}{
		"já existe (corrida)": {domainerrors.ErrAlreadyExists, false},
		"inbox duplicado":     {domainerrors.ErrDuplicateMessage, false},
		"falha de infra":      {errors.New("dynamodb fora"), true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			repo := newRepo(t)
			repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
			repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound)
			repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(c.commitErr)

			err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput())
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}

func TestStartDiagnosis_ConflitoDeVersaoReaplica(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(conflictAttempts)
	repo.EXPECT().Get(gomock.Any(), "os-1").Return(nil, domainerrors.ErrNotFound).Times(conflictAttempts)
	repo.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(domainerrors.ErrConcurrentUpdate).Times(conflictAttempts)

	err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput())
	if !errors.Is(err, domainerrors.ErrConcurrentUpdate) {
		t.Fatalf("depois de %d tentativas o erro volta para a fila; veio %v", conflictAttempts, err)
	}
}

func TestStartDiagnosis_Erros(t *testing.T) {
	boom := errors.New("boom")

	t.Run("inbox indisponível", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, boom)
		if err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput()); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("leitura falhou", func(t *testing.T) {
		repo := newRepo(t)
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, boom)
		if err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), startDiagnosisInput()); !errors.Is(err, boom) {
			t.Fatalf("esperava boom, veio %v", err)
		}
	})
	t.Run("sem os_id", func(t *testing.T) {
		repo := newRepo(t)
		in := startDiagnosisInput()
		in.Message.OSID = ""
		repo.EXPECT().IsProcessed(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		repo.EXPECT().Get(gomock.Any(), "").Return(nil, domainerrors.ErrNotFound)
		if err := NewStartDiagnosis(testDeps(repo)).Execute(context.Background(), in); !errors.Is(err, domainerrors.ErrInvalidExecution) {
			t.Fatalf("esperava ErrInvalidExecution, veio %v", err)
		}
	})
}

func TestNewBase_RelogioPadrao(t *testing.T) {
	b := newBase(Deps{})
	if b.now().Location().String() != "UTC" {
		t.Fatal("o relógio padrão deve devolver UTC")
	}
}
