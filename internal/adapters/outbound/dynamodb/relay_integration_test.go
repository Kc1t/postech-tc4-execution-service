package dynamodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/newrelic/go-agent/v3/newrelic"
)

func disabledNR(t *testing.T) *newrelic.Application {
	t.Helper()
	app, err := newrelic.NewApplication(newrelic.ConfigAppName("test"), newrelic.ConfigEnabled(false))
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func readOutbox(t *testing.T, repo *Repository) []outboxItem {
	t.Helper()
	var out []outboxItem
	err := paginate(context.Background(), testClient, &ddb.QueryInput{
		TableName:                 aws.String(repo.Table()),
		KeyConditionExpression:    aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]typesAV{":pk": str(pkOutbox)},
		ConsistentRead:            aws.Bool(true),
	}, func(item map[string]typesAV) (bool, error) {
		var m outboxItem
		if err := attributevalue.UnmarshalMap(item, &m); err != nil {
			return false, err
		}
		out = append(out, m)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestIntegration_RelayPublicaEmOrdem(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	app := disabledNR(t)
	txn := app.StartTransaction("http")
	reqCtx := newrelic.NewContext(ctx, txn)

	m1 := outboxMsg(t, contracts.EvtDiagnosisStarted, "os-r")
	m2 := outboxMsg(t, contracts.EvtDiagnosisCompleted, "os-r")
	if err := repo.Commit(reqCtx, ports.Write{Outbox: []ports.OutboxMessage{m1}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := repo.Commit(reqCtx, ports.Write{Outbox: []ports.OutboxMessage{m2}}); err != nil {
		t.Fatal(err)
	}
	txn.End()

	pub := messaging.NewMemoryPublisher()
	relay := NewRelay(testClient, repo.Table(), pub, app, nil)
	if n, _ := relay.Pending(ctx); n != 2 {
		t.Fatalf("pendentes = %d", n)
	}
	n, err := relay.Flush(ctx)
	if err != nil || n != 2 {
		t.Fatalf("Flush: n=%d err=%v", n, err)
	}
	msgs := pub.Messages()
	if len(msgs) != 2 || msgs[0].Envelope.MessageID != m1.Envelope.MessageID || msgs[1].Envelope.MessageID != m2.Envelope.MessageID {
		t.Fatalf("publicação fora de ordem: %+v", msgs)
	}
	if msgs[0].Dest != topic || msgs[0].Envelope.Type != contracts.EvtDiagnosisStarted || msgs[0].Envelope.OSID != "os-r" {
		t.Fatalf("destino ou envelope errado: %+v", msgs[0])
	}
	if p, _ := relay.Pending(ctx); p != 0 {
		t.Fatalf("ainda há %d pendentes", p)
	}
	if n, _ := relay.Flush(ctx); n != 0 {
		t.Fatal("não pode republicar mensagens já publicadas")
	}
	for _, m := range readOutbox(t, repo) {
		if !m.Published || m.PublishedAt == nil || m.TTL == 0 || m.LeaseOwner != "" || m.LeaseUntil != 0 {
			t.Fatalf("mensagem publicada sem marcação completa: %+v", m)
		}
	}
}

func TestIntegration_RelayFalhaRegistraTentativa(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	if err := repo.Commit(ctx, ports.Write{Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtRepairFinished, "os-f")}}); err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewMemoryPublisher()
	pub.Err = errors.New("sns fora")
	relay := NewRelay(testClient, repo.Table(), pub, nil, nil)
	start := time.Now().UTC()
	relay.now = func() time.Time { return start } // relógio parado: o LocalStack pode levar mais que a espera de 1 s

	if n, err := relay.Flush(ctx); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	items := readOutbox(t, repo)
	if len(items) != 1 || items[0].Attempts != 1 || items[0].LastError != "sns fora" || items[0].Published || items[0].LeaseUntil == 0 {
		t.Fatalf("falha não registrada ou sem hora da próxima tentativa: %+v", items)
	}

	pub.Err = nil
	if n, _ := relay.Flush(ctx); n != 0 {
		t.Fatal("não pode tentar de novo antes de a espera passar")
	}
	relay.now = func() time.Time { return start.Add(2 * time.Second) }
	if n, _ := relay.Flush(ctx); n != 1 {
		t.Fatal("deveria publicar na tentativa seguinte")
	}
	items = readOutbox(t, repo)
	if !items[0].Published || items[0].LastError != "" {
		t.Fatalf("depois do sucesso o erro anterior é limpo: %+v", items[0])
	}
}

func TestIntegration_RelayReservaEntreReplicas(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	if err := repo.Commit(ctx, ports.Write{Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtRepairStarted, "os-l")}}); err != nil {
		t.Fatal(err)
	}
	a := NewRelay(testClient, repo.Table(), messaging.NewMemoryPublisher(), nil, nil)
	pubB := messaging.NewMemoryPublisher()
	b := NewRelay(testClient, repo.Table(), pubB, nil, nil)

	pending, err := a.pending(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending: %v %v", pending, err)
	}
	if ok, err := a.claim(ctx, &pending[0]); err != nil || !ok {
		t.Fatalf("réplica A deveria reservar: %v %v", ok, err)
	}
	if n, err := b.Flush(ctx); err != nil || n != 0 || len(pubB.Messages()) != 0 {
		t.Fatalf("réplica B não pode publicar mensagem reservada: n=%d err=%v", n, err)
	}
	if ok, _ := b.claim(ctx, &pending[0]); ok {
		t.Fatal("reserva dupla")
	}

	// Lease vencido (réplica A caiu): B assume.
	b.now = func() time.Time { return time.Now().UTC().Add(time.Minute) }
	if n, err := b.Flush(ctx); err != nil || n != 1 {
		t.Fatalf("depois do lease vencido B publica: n=%d err=%v", n, err)
	}
}

func TestIntegration_RelayCorpoInvalido(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	bad := outboxMsg(t, "x", "os-i")
	bad.Envelope.MessageID = "" // Parse recusa envelope sem message_id
	if err := repo.Commit(ctx, ports.Write{Outbox: []ports.OutboxMessage{bad}}); err != nil {
		t.Fatal(err)
	}
	relay := NewRelay(testClient, repo.Table(), messaging.NewMemoryPublisher(), nil, nil)
	if n, err := relay.Flush(ctx); err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if items := readOutbox(t, repo); items[0].Attempts != 1 || items[0].LastError == "" {
		t.Fatalf("corpo inválido deve ficar registrado como falha: %+v", items[0])
	}
}

func TestIntegration_RelayRunParaComContexto(t *testing.T) {
	repo := newTable(t)
	if err := repo.Commit(context.Background(), ports.Write{Outbox: []ports.OutboxMessage{outboxMsg(t, contracts.EvtRepairFinished, "os-run")}}); err != nil {
		t.Fatal(err)
	}
	pub := messaging.NewMemoryPublisher()
	relay := NewRelay(testClient, repo.Table(), pub, nil, nil).WithInterval(20 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { relay.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(pub.Messages()) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if len(pub.Messages()) != 1 {
		t.Fatal("relay em loop não publicou")
	}
}

// failingPublisher falha só para as mensagens de uma OS.
type failingPublisher struct {
	*messaging.MemoryPublisher
	badOS string
}

func (p failingPublisher) Publish(ctx context.Context, dest messaging.Destination, env contracts.Envelope) error {
	if env.OSID == p.badOS {
		return errors.New("destino fora")
	}
	return p.MemoryPublisher.Publish(ctx, dest, env)
}

func TestIntegration_RelayFalhaNaoTravaOLote(t *testing.T) {
	repo := newTable(t)
	ctx := context.Background()
	ruim, boa := outboxMsg(t, contracts.EvtRepairFinished, "os-ruim"), outboxMsg(t, contracts.EvtRepairFinished, "os-boa")
	if err := repo.Commit(ctx, ports.Write{Outbox: []ports.OutboxMessage{ruim}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Commit(ctx, ports.Write{Outbox: []ports.OutboxMessage{boa}}); err != nil {
		t.Fatal(err)
	}
	pub := failingPublisher{MemoryPublisher: messaging.NewMemoryPublisher(), badOS: "os-ruim"}
	relay := NewRelay(testClient, repo.Table(), pub, nil, nil)
	relay.batch = 1 // o lote só cabe a mensagem mais antiga, a que falha

	if n, _ := relay.Flush(ctx); n != 0 {
		t.Fatal("a primeira varredura pega só a mensagem que falha")
	}
	if n, _ := relay.Flush(ctx); n != 1 {
		t.Fatal("em espera, a mensagem que falhou não pode ocupar o lote da próxima varredura")
	}
	if msgs := pub.Messages(); len(msgs) != 1 || msgs[0].Envelope.OSID != "os-boa" {
		t.Fatalf("publicou %+v", msgs)
	}
}
