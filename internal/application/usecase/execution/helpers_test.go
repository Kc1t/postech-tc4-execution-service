package executionuc

import (
	"context"
	"testing"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports/mocks"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"go.uber.org/mock/gomock"
)

var (
	fixedNow   = time.Date(2026, 11, 5, 14, 30, 0, 0, time.UTC)
	eventsDest = messaging.Destination{Kind: messaging.Topic, Name: "arn:aws:sns:us-east-1:000000000000:execution-events"}
	sagaActor  = entities.ServiceActor("os-service")
	mechanic   = entities.Actor{ID: "mec-1", Type: entities.ActorMechanic}
)

func newRepo(t *testing.T) *mocks.MockExecutionRepository {
	t.Helper()
	return mocks.NewMockExecutionRepository(gomock.NewController(t))
}

func testDeps(repo ports.ExecutionRepository) Deps {
	return Deps{Repo: repo, Events: eventsDest, Now: func() time.Time { return fixedNow }}
}

func msgMeta(msgType string) ports.MessageMeta {
	return ports.MessageMeta{
		MessageID: "msg-1", Type: msgType, SagaID: "saga-1", OSID: "os-1",
		CorrelationID: "corr-1", Producer: "os-service",
	}
}

// execAt devolve uma execução persistida (Version > 0, sem eventos pendentes) no status pedido.
func execAt(t *testing.T, s entities.Status) *entities.Execution {
	t.Helper()
	e, err := entities.NewExecution(entities.NewExecutionParams{
		OSID: "os-1", OSCode: 42, SagaID: "saga-1", Vehicle: entities.Vehicle{Plate: "ABC1D23"},
	}, sagaActor, fixedNow.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	at := fixedNow.Add(-30 * time.Minute)
	steps := []func() error{
		func() error { return e.BeginDiagnosis(mechanic, at) },
		func() error {
			return e.CompleteDiagnosis(entities.Diagnosis{Services: []entities.DiagnosedService{{Code: 1}}}, mechanic, at)
		},
		func() error { return e.EnqueueRepair(nil, sagaActor, at) },
		func() error { return e.BeginRepair(mechanic, at) },
		func() error { return e.FinishRepair(mechanic, at) },
	}
	order := []entities.Status{entities.StatusInDiagnosis, entities.StatusDiagnosed, entities.StatusQueuedRepair, entities.StatusInRepair, entities.StatusFinished}
	switch s {
	case entities.StatusQueuedDiagnosis:
	case entities.StatusCancelled:
		if err := e.Cancel("x", sagaActor, at); err != nil {
			t.Fatal(err)
		}
	case entities.StatusRejected:
		_ = steps[0]()
		_ = steps[1]()
		if err := e.RejectRepair("x", sagaActor, at); err != nil {
			t.Fatal(err)
		}
	default:
		for i, step := range steps {
			if err := step(); err != nil {
				t.Fatal(err)
			}
			if order[i] == s {
				break
			}
		}
	}
	if e.Status != s {
		t.Fatalf("execAt: esperava %s, ficou %s", s, e.Status)
	}
	e.Version = 3
	e.ClearPendingEvents()
	return e
}

// capture grava a última Write recebida pelo Commit.
func capture(w *ports.Write, err error) func(context.Context, ports.Write) error {
	return func(_ context.Context, got ports.Write) error {
		*w = got
		return err
	}
}

func decode[T any](t *testing.T, m ports.OutboxMessage) T {
	t.Helper()
	v, err := contracts.Decode[T](m.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
