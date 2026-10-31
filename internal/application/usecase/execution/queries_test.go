package executionuc

import (
	"context"
	"errors"
	"testing"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"go.uber.org/mock/gomock"
)

func TestGetQueue_ReparoAntesDeDiagnostico(t *testing.T) {
	repo := newRepo(t)
	r1, r2 := execAt(t, entities.StatusQueuedRepair), execAt(t, entities.StatusQueuedRepair)
	d1 := execAt(t, entities.StatusQueuedDiagnosis)
	gomock.InOrder(
		repo.EXPECT().ListByStatus(gomock.Any(), entities.StatusQueuedRepair, DefaultQueueLimit).Return([]*entities.Execution{r1, r2}, nil),
		repo.EXPECT().ListByStatus(gomock.Any(), entities.StatusQueuedDiagnosis, DefaultQueueLimit-2).Return([]*entities.Execution{d1}, nil),
	)
	got, err := NewGetQueue(repo).Execute(context.Background(), ports.QueueQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != r1 || got[1] != r2 || got[2] != d1 {
		t.Fatalf("ordem errada: %v", got)
	}
}

func TestGetQueue_LimiteAtingidoPeloReparo(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().ListByStatus(gomock.Any(), entities.StatusQueuedRepair, 1).Return([]*entities.Execution{execAt(t, entities.StatusQueuedRepair)}, nil)
	got, err := NewGetQueue(repo).Execute(context.Background(), ports.QueueQuery{Limit: 1})
	if err != nil || len(got) != 1 {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestGetQueue_FiltroPorStatus(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().ListByStatus(gomock.Any(), entities.StatusInRepair, MaxQueueLimit).Return(nil, nil)
	if _, err := NewGetQueue(repo).Execute(context.Background(), ports.QueueQuery{Status: entities.StatusInRepair, Limit: 10_000}); err != nil {
		t.Fatal(err)
	}
}

func TestGetQueue_StatusFinalNaoTemFila(t *testing.T) {
	_, err := NewGetQueue(newRepo(t)).Execute(context.Background(), ports.QueueQuery{Status: entities.StatusFinished})
	if !errors.Is(err, domainerrors.ErrInvalidStatus) {
		t.Fatalf("esperava ErrInvalidStatus, veio %v", err)
	}
}

func TestGetQueue_Erro(t *testing.T) {
	repo := newRepo(t)
	boom := errors.New("boom")
	repo.EXPECT().ListByStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, boom)
	if _, err := NewGetQueue(repo).Execute(context.Background(), ports.QueueQuery{}); !errors.Is(err, boom) {
		t.Fatalf("esperava boom, veio %v", err)
	}
}

func TestGetExecution(t *testing.T) {
	repo := newRepo(t)
	exec := execAt(t, entities.StatusInRepair)
	timeline := []entities.TimelineEvent{{ID: "e1", Type: entities.EventDiagnosisQueued}}
	repo.EXPECT().GetWithTimeline(gomock.Any(), "os-1").Return(exec, timeline, nil)

	got, err := NewGetExecution(repo).Execute(context.Background(), "os-1")
	if err != nil || got.Execution != exec || len(got.Timeline) != 1 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestGetExecution_NaoEncontrada(t *testing.T) {
	repo := newRepo(t)
	repo.EXPECT().GetWithTimeline(gomock.Any(), "os-x").Return(nil, nil, domainerrors.ErrNotFound)
	if _, err := NewGetExecution(repo).Execute(context.Background(), "os-x"); !errors.Is(err, domainerrors.ErrNotFound) {
		t.Fatalf("esperava ErrNotFound, veio %v", err)
	}
}
