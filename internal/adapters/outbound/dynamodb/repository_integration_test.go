package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
)

var topic = messaging.Destination{Kind: messaging.Topic, Name: "arn:aws:sns:us-east-1:000000000000:execution-events"}

func outboxMsg(t *testing.T, msgType, osID string) ports.OutboxMessage {
	t.Helper()
	env, err := contracts.NewEnvelope(msgType, "execution-service", contracts.Meta{SagaID: "saga-" + osID, OSID: osID}, map[string]string{"k": "v"})
	if err != nil {
		t.Fatal(err)
	}
	return ports.OutboxMessage{Destination: topic, Envelope: env}
}

func TestIntegration_CriaELeExecucao(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	e := newExecution(t, "os-a", baseTime)

	if err := repo.Commit(ctx, ports.Write{Execution: e}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if e.Version != 1 || len(e.PendingEvents()) != 0 {
		t.Fatalf("Commit deve incrementar a versão e limpar os eventos: v=%d pend=%d", e.Version, len(e.PendingEvents()))
	}
	got, err := repo.Get(ctx, "os-a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.OSID != "os-a" || got.OSCode != 7 || got.SagaID != "saga-os-a" || got.Vehicle != e.Vehicle ||
		got.Notes != "revisão" || got.Status != entities.StatusQueuedDiagnosis || got.Version != 1 ||
		!got.EnqueuedAt.Equal(baseTime) || !got.CreatedAt.Equal(baseTime) {
		t.Fatalf("execução lida difere da gravada:\n got=%+v\nwant=%+v", got, e)
	}
}

func TestIntegration_RoundTripCompleto(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	e := newExecution(t, "os-b", baseTime)
	steps := []func() error{
		func() error { return e.BeginDiagnosis(mechanic, baseTime.Add(time.Minute)) },
		func() error {
			return e.CompleteDiagnosis(entities.Diagnosis{
				Services: []entities.DiagnosedService{{Code: 10}},
				Parts:    []entities.DiagnosedPart{{ManufacturerCode: "NGK-1", Quantity: 4}},
				Notes:    "velas",
			}, mechanic, baseTime.Add(2*time.Minute))
		},
		func() error {
			return e.EnqueueRepair([]entities.RepairItem{{Kind: "part", Ref: "NGK-1", Description: "Vela", Quantity: 4, UnitPrice: 35.5}}, saga, baseTime.Add(3*time.Minute))
		},
		func() error { return e.BeginRepair(mechanic, baseTime.Add(4*time.Minute)) },
		func() error { return e.FinishRepair(mechanic, baseTime.Add(5*time.Minute)) },
	}
	if err := repo.Commit(ctx, ports.Write{Execution: e}); err != nil {
		t.Fatal(err)
	}
	for i, step := range steps {
		if err := step(); err != nil {
			t.Fatal(err)
		}
		if err := repo.Commit(ctx, ports.Write{Execution: e}); err != nil {
			t.Fatalf("passo %d: %v", i, err)
		}
	}

	got, timeline, err := repo.GetWithTimeline(ctx, "os-b")
	if err != nil {
		t.Fatalf("GetWithTimeline: %v", err)
	}
	if got.Status != entities.StatusFinished || got.Version != 6 {
		t.Fatalf("estado final errado: %s v%d", got.Status, got.Version)
	}
	if got.Diagnosis == nil || !reflect.DeepEqual(got.Diagnosis.Parts, []entities.DiagnosedPart{{ManufacturerCode: "NGK-1", Quantity: 4}}) ||
		got.Diagnosis.Notes != "velas" || !got.Diagnosis.CompletedAt.Equal(baseTime.Add(2*time.Minute)) {
		t.Fatalf("diagnóstico errado: %+v", got.Diagnosis)
	}
	if len(got.Items) != 1 || got.Items[0].UnitPrice != 35.5 || got.Items[0].Description != "Vela" {
		t.Fatalf("itens errados: %+v", got.Items)
	}
	if got.DiagnosisStartedAt == nil || got.DiagnosedAt == nil || got.RepairStartedAt == nil || got.FinishedAt == nil ||
		!got.FinishedAt.Equal(baseTime.Add(5*time.Minute)) {
		t.Fatalf("marcos de tempo errados: %+v", got)
	}
	wantTypes := []entities.EventType{
		entities.EventDiagnosisQueued, entities.EventDiagnosisStarted, entities.EventDiagnosisCompleted,
		entities.EventRepairQueued, entities.EventRepairStarted, entities.EventRepairFinished,
	}
	if len(timeline) != len(wantTypes) {
		t.Fatalf("esperava %d eventos, veio %d", len(wantTypes), len(timeline))
	}
	for i, w := range wantTypes {
		if timeline[i].Type != w {
			t.Fatalf("evento %d = %s, esperava %s (ordem cronológica)", i, timeline[i].Type, w)
		}
	}
	if timeline[1].Actor != mechanic || timeline[2].Details["parts"] != "1" || timeline[0].From != "" || timeline[5].To != entities.StatusFinished {
		t.Fatalf("detalhes da linha do tempo errados: %+v", timeline)
	}
}

func TestIntegration_CriacaoDuplicada(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	if err := repo.Commit(ctx, ports.Write{Execution: newExecution(t, "os-c", baseTime)}); err != nil {
		t.Fatal(err)
	}
	err := repo.Commit(ctx, ports.Write{Execution: newExecution(t, "os-c", baseTime)})
	if !errors.Is(err, domainerrors.ErrAlreadyExists) {
		t.Fatalf("esperava ErrAlreadyExists, veio %v", err)
	}
}

func TestIntegration_OptimisticLock(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	if err := repo.Commit(ctx, ports.Write{Execution: newExecution(t, "os-d", baseTime)}); err != nil {
		t.Fatal(err)
	}
	a, _ := repo.Get(ctx, "os-d")
	b, _ := repo.Get(ctx, "os-d")

	if err := a.BeginDiagnosis(mechanic, baseTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Commit(ctx, ports.Write{Execution: a, Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtDiagnosisStarted, "os-d")}}); err != nil {
		t.Fatal(err)
	}

	if err := b.Cancel("x", saga, baseTime.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	err := repo.Commit(ctx, ports.Write{Execution: b, Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtExecutionCancelled, "os-d")}})
	if !errors.Is(err, domainerrors.ErrConcurrentUpdate) {
		t.Fatalf("esperava ErrConcurrentUpdate, veio %v", err)
	}
	if b.Version != 1 || len(b.PendingEvents()) == 0 {
		t.Fatal("commit recusado não pode alterar versão nem descartar eventos pendentes")
	}

	got, timeline, _ := repo.GetWithTimeline(ctx, "os-d")
	if got.Status != entities.StatusInDiagnosis || got.Version != 2 || len(timeline) != 2 {
		t.Fatalf("a transação recusada não pode gravar nada: status=%s v=%d eventos=%d", got.Status, got.Version, len(timeline))
	}
	relay := NewRelay(testClient, repo.Table(), messaging.NewMemoryPublisher(), nil, nil)
	if n, _ := relay.Pending(ctx); n != 1 {
		t.Fatalf("só o evento da transação confirmada fica no outbox; pendentes=%d", n)
	}
}

func TestIntegration_ConcorrenciaUmVence(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	if err := repo.Commit(ctx, ports.Write{Execution: newExecution(t, "os-e", baseTime)}); err != nil {
		t.Fatal(err)
	}
	// Todos leem a mesma versão antes de qualquer um gravar: só um commit pode vencer.
	const workers = 5
	copies := make([]*entities.Execution, workers)
	for i := range copies {
		e, err := repo.Get(ctx, "os-e")
		if err != nil {
			t.Fatal(err)
		}
		if err := e.BeginDiagnosis(entities.Actor{ID: fmt.Sprintf("mec-%d", i), Type: entities.ActorMechanic}, baseTime); err != nil {
			t.Fatal(err)
		}
		copies[i] = e
	}
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		ok, confl int
	)
	start := make(chan struct{})
	for _, e := range copies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := repo.Commit(ctx, ports.Write{Execution: e})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, domainerrors.ErrConcurrentUpdate):
				confl++
			default:
				t.Errorf("erro inesperado: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || confl != workers-1 {
		t.Fatalf("esperava 1 vencedor e %d conflitos: ok=%d conflitos=%d", workers-1, ok, confl)
	}
	got, timeline, err := repo.GetWithTimeline(ctx, "os-e")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 2 || got.Status != entities.StatusInDiagnosis || len(timeline) != 2 {
		t.Fatalf("só o vencedor grava estado e evento: v%d %s, %d eventos", got.Version, got.Status, len(timeline))
	}
}

func TestIntegration_InboxDuplicado(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	in := &ports.InboxEntry{Consumer: "execution-commands", MessageID: "m-1", Type: contracts.CmdStartDiagnosis}

	if done, err := repo.IsProcessed(ctx, in.Consumer, in.MessageID); err != nil || done {
		t.Fatalf("IsProcessed antes: %v %v", done, err)
	}
	if err := repo.Commit(ctx, ports.Write{Execution: newExecution(t, "os-f", baseTime), Inbox: in}); err != nil {
		t.Fatal(err)
	}
	if done, err := repo.IsProcessed(ctx, in.Consumer, in.MessageID); err != nil || !done {
		t.Fatalf("IsProcessed depois: %v %v", done, err)
	}

	e, _ := repo.Get(ctx, "os-f")
	_ = e.Cancel("x", saga, baseTime)
	err := repo.Commit(ctx, ports.Write{Execution: e, Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtExecutionCancelled, "os-f")}, Inbox: in})
	if !errors.Is(err, domainerrors.ErrDuplicateMessage) {
		t.Fatalf("esperava ErrDuplicateMessage, veio %v", err)
	}
	got, _ := repo.Get(ctx, "os-f")
	if got.Status != entities.StatusQueuedDiagnosis || got.Version != 1 {
		t.Fatalf("mensagem duplicada não pode aplicar efeitos: %s v%d", got.Status, got.Version)
	}
	// Mesmo message_id em outro consumidor é outra entrega.
	other := &ports.InboxEntry{Consumer: "outro", MessageID: "m-1", Type: "x"}
	if err := repo.Commit(ctx, ports.Write{Inbox: other}); err != nil {
		t.Fatalf("inbox é por consumidor: %v", err)
	}
}

func TestIntegration_SoMensagens(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	in := &ports.InboxEntry{Consumer: "execution-commands", MessageID: "m-2", Type: contracts.CmdStartRepair}
	if err := repo.Commit(ctx, ports.Write{Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtRepairRejected, "os-x")}, Inbox: in}); err != nil {
		t.Fatal(err)
	}
	relay := NewRelay(testClient, repo.Table(), messaging.NewMemoryPublisher(), nil, nil)
	if n, _ := relay.Pending(ctx); n != 1 {
		t.Fatalf("pendentes = %d", n)
	}
	if _, err := repo.Get(ctx, "os-x"); !errors.Is(err, domainerrors.ErrNotFound) {
		t.Fatalf("nenhum estado deveria ser criado: %v", err)
	}
	if err := repo.Commit(ctx, ports.Write{}); err != nil {
		t.Fatalf("unidade de trabalho vazia é no-op: %v", err)
	}
}

func TestIntegration_FilaPorPrioridadeEChegada(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	// Gravadas fora de ordem: a fila ordena pela chegada (enqueued_at), não pela gravação.
	for _, c := range []struct {
		id  string
		off time.Duration
	}{{"os-3", 3 * time.Minute}, {"os-1", time.Minute}, {"os-2", 2 * time.Minute}} {
		if err := repo.Commit(ctx, ports.Write{Execution: newExecution(t, c.id, baseTime.Add(c.off))}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.ListByStatus(ctx, entities.StatusQueuedDiagnosis, 10)
	if err != nil {
		t.Fatal(err)
	}
	if ids(got) != "os-1,os-2,os-3" {
		t.Fatalf("ordem errada: %s", ids(got))
	}
	if two, _ := repo.ListByStatus(ctx, entities.StatusQueuedDiagnosis, 2); ids(two) != "os-1,os-2" {
		t.Fatalf("limite não respeitado: %s", ids(two))
	}
	if none, _ := repo.ListByStatus(ctx, entities.StatusQueuedDiagnosis, 0); len(none) != 0 {
		t.Fatal("limite 0 devolve lista vazia")
	}

	// os-1 sai da fila de diagnóstico ao ser iniciado; os-2 é cancelado e sai do índice.
	e1, _ := repo.Get(ctx, "os-1")
	_ = e1.BeginDiagnosis(mechanic, baseTime.Add(10*time.Minute))
	e2, _ := repo.Get(ctx, "os-2")
	_ = e2.Cancel("x", saga, baseTime.Add(10*time.Minute))
	for _, e := range []*entities.Execution{e1, e2} {
		if err := repo.Commit(ctx, ports.Write{Execution: e}); err != nil {
			t.Fatal(err)
		}
	}
	if q, _ := repo.ListByStatus(ctx, entities.StatusQueuedDiagnosis, 10); ids(q) != "os-3" {
		t.Fatalf("fila de diagnóstico = %s", ids(q))
	}
	if q, _ := repo.ListByStatus(ctx, entities.StatusInDiagnosis, 10); ids(q) != "os-1" {
		t.Fatalf("em diagnóstico = %s", ids(q))
	}
	if q, _ := repo.ListByStatus(ctx, entities.StatusCancelled, 10); len(q) != 0 {
		t.Fatal("status final não fica no índice (gsi1 esparso)")
	}
	n, err := repo.CountByStatus(ctx, entities.StatusQueuedDiagnosis, entities.StatusInDiagnosis, entities.StatusCancelled)
	if err != nil || n != 2 {
		t.Fatalf("CountByStatus = %d, %v", n, err)
	}
}

func TestIntegration_NaoEncontrada(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	if _, err := repo.Get(ctx, "nada"); !errors.Is(err, domainerrors.ErrNotFound) {
		t.Fatalf("Get: %v", err)
	}
	if _, _, err := repo.GetWithTimeline(ctx, "nada"); !errors.Is(err, domainerrors.ErrNotFound) {
		t.Fatalf("GetWithTimeline: %v", err)
	}
}

func TestIntegration_Ping(t *testing.T) {
	repo := newTable(t)
	if err := repo.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if err := NewRepository(testClient, "tabela-que-nao-existe").Ping(context.Background()); err == nil {
		t.Fatal("Ping deveria falhar para tabela inexistente")
	}
}

func ids(es []*entities.Execution) string {
	s := ""
	for i, e := range es {
		if i > 0 {
			s += ","
		}
		s += e.OSID
	}
	return s
}
