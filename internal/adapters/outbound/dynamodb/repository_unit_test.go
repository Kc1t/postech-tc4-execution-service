package dynamodb

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type typesAV = types.AttributeValue

// fakeAPI devolve respostas programadas; serve para os caminhos de erro sem LocalStack.
type fakeAPI struct {
	getItem  func(*ddb.GetItemInput) (*ddb.GetItemOutput, error)
	query    func(*ddb.QueryInput) (*ddb.QueryOutput, error)
	transact func(*ddb.TransactWriteItemsInput) (*ddb.TransactWriteItemsOutput, error)
	update   func(*ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error)
	describe func(*ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error)
}

func (f *fakeAPI) GetItem(_ context.Context, in *ddb.GetItemInput, _ ...func(*ddb.Options)) (*ddb.GetItemOutput, error) {
	return f.getItem(in)
}
func (f *fakeAPI) Query(_ context.Context, in *ddb.QueryInput, _ ...func(*ddb.Options)) (*ddb.QueryOutput, error) {
	return f.query(in)
}
func (f *fakeAPI) TransactWriteItems(_ context.Context, in *ddb.TransactWriteItemsInput, _ ...func(*ddb.Options)) (*ddb.TransactWriteItemsOutput, error) {
	return f.transact(in)
}
func (f *fakeAPI) UpdateItem(_ context.Context, in *ddb.UpdateItemInput, _ ...func(*ddb.Options)) (*ddb.UpdateItemOutput, error) {
	return f.update(in)
}
func (f *fakeAPI) DescribeTable(_ context.Context, in *ddb.DescribeTableInput, _ ...func(*ddb.Options)) (*ddb.DescribeTableOutput, error) {
	return f.describe(in)
}

var errBoom = errors.New("boom")

func reasons(codes ...string) []types.CancellationReason {
	out := make([]types.CancellationReason, len(codes))
	for i, c := range codes {
		out[i] = types.CancellationReason{Code: aws.String(c)}
	}
	return out
}

func TestMapTxError(t *testing.T) {
	roles := []txItemRole{roleState, roleTimeline, roleOutbox, roleInbox}
	cases := map[string]struct {
		err      error
		creating bool
		want     error
	}{
		"inbox duplicado tem precedência": {
			&types.TransactionCanceledException{CancellationReasons: reasons("ConditionalCheckFailed", "None", "None", "ConditionalCheckFailed")},
			false, domainerrors.ErrDuplicateMessage,
		},
		"versão divergente": {
			&types.TransactionCanceledException{CancellationReasons: reasons("ConditionalCheckFailed", "None", "None", "None")},
			false, domainerrors.ErrConcurrentUpdate,
		},
		"criação repetida": {
			&types.TransactionCanceledException{CancellationReasons: reasons("ConditionalCheckFailed", "None", "None", "None")},
			true, domainerrors.ErrAlreadyExists,
		},
		"conflito de transação": {
			&types.TransactionCanceledException{CancellationReasons: reasons("None", "TransactionConflict", "None", "None")},
			false, domainerrors.ErrConcurrentUpdate,
		},
		"motivos só na mensagem": {
			&types.TransactionCanceledException{Message: aws.String("Transaction cancelled, please refer cancellation reasons for specific reasons [None, None, None, ConditionalCheckFailed]")},
			false, domainerrors.ErrDuplicateMessage,
		},
		"TransactionConflictException":   {&types.TransactionConflictException{}, false, domainerrors.ErrConcurrentUpdate},
		"TransactionInProgressException": {&types.TransactionInProgressException{}, false, domainerrors.ErrConcurrentUpdate},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got := mapTxError(c.err, roles, c.creating, "os-1"); !errors.Is(got, c.want) {
				t.Fatalf("esperava %v, veio %v", c.want, got)
			}
		})
	}

	t.Run("outros erros não viram erro de domínio", func(t *testing.T) {
		for _, err := range []error{
			errBoom,
			&types.TransactionCanceledException{CancellationReasons: reasons("ValidationError")},
			&types.TransactionCanceledException{Message: aws.String("sem lista")},
		} {
			got := mapTxError(err, roles, false, "os-1")
			if errors.Is(got, domainerrors.ErrConcurrentUpdate) || errors.Is(got, domainerrors.ErrDuplicateMessage) || !errors.Is(got, err) {
				t.Fatalf("erro %v mapeado errado: %v", err, got)
			}
		}
	})
}

func TestChavesOrdenaveis(t *testing.T) {
	a := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	b := a.Add(time.Nanosecond)
	c := a.Add(time.Hour)
	keys := []string{outboxSK(c, "x"), outboxSK(b, "x"), outboxSK(a, "x")}
	sort.Strings(keys)
	if keys[0] != outboxSK(a, "x") || keys[2] != outboxSK(c, "x") {
		t.Fatalf("ordem lexicográfica deve ser a cronológica: %v", keys)
	}
	if queueSK(entities.PriorityRepair, c) >= queueSK(entities.PriorityDiagnosis, a) {
		t.Fatal("prioridade vem antes da chegada na sort key")
	}
	if eventSK(a, "id") >= skState {
		t.Fatal("eventos devem vir antes do STATE na coleção")
	}
	if execPK("1") != "EXEC#1" || queuePK(entities.StatusQueuedRepair) != "QUEUE#queued_repair" || inboxPK("c", "m") != "MSG#c#m" {
		t.Fatal("formato de chave errado")
	}
}

func TestMapeamentoDoEstado(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e, _ := entities.NewExecution(entities.NewExecutionParams{OSID: "os-1", OSCode: 1, SagaID: "s"}, saga, now)
	_ = e.BeginDiagnosis(mechanic, now)
	_ = e.CompleteDiagnosis(entities.Diagnosis{Services: []entities.DiagnosedService{{Code: 3}}}, mechanic, now)
	_ = e.EnqueueRepair([]entities.RepairItem{{Kind: "service", Ref: "3", Quantity: 1}}, saga, now)
	e.Version = 4

	it := toStateItem(e, 5)
	if it.GSI1PK != "QUEUE#queued_repair" || it.GSI1SK != queueSK(entities.PriorityRepair, now) || it.Priority != entities.PriorityRepair || it.Version != 5 {
		t.Fatalf("item errado: %+v", it)
	}
	back := it.toEntity()
	if back.Version != 5 || back.Status != e.Status || !reflect.DeepEqual(back.Diagnosis, e.Diagnosis) || !reflect.DeepEqual(back.Items, e.Items) {
		t.Fatalf("ida e volta perdeu dados:\n%+v\n%+v", back, e)
	}

	_ = e.BeginRepair(mechanic, now)
	_ = e.FinishRepair(mechanic, now)
	if fin := toStateItem(e, 6); fin.GSI1PK != "" || fin.GSI1SK != "" {
		t.Fatal("execução finalizada não entra no gsi1")
	}
}

func TestRepository_ErrosDaAPI(t *testing.T) {
	ctx := context.Background()
	fake := &fakeAPI{
		getItem:  func(*ddb.GetItemInput) (*ddb.GetItemOutput, error) { return nil, errBoom },
		query:    func(*ddb.QueryInput) (*ddb.QueryOutput, error) { return nil, errBoom },
		transact: func(*ddb.TransactWriteItemsInput) (*ddb.TransactWriteItemsOutput, error) { return nil, errBoom },
		describe: func(*ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error) { return nil, errBoom },
	}
	repo := NewRepository(fake, "t")
	if _, err := repo.Get(ctx, "x"); !errors.Is(err, errBoom) {
		t.Errorf("Get: %v", err)
	}
	if _, _, err := repo.GetWithTimeline(ctx, "x"); !errors.Is(err, errBoom) {
		t.Errorf("GetWithTimeline: %v", err)
	}
	if _, err := repo.ListByStatus(ctx, entities.StatusQueuedRepair, 5); !errors.Is(err, errBoom) {
		t.Errorf("ListByStatus: %v", err)
	}
	if _, err := repo.CountByStatus(ctx, entities.StatusInRepair); !errors.Is(err, errBoom) {
		t.Errorf("CountByStatus: %v", err)
	}
	if _, err := repo.IsProcessed(ctx, "c", "m"); !errors.Is(err, errBoom) {
		t.Errorf("IsProcessed: %v", err)
	}
	if err := repo.Ping(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Ping: %v", err)
	}
	e, _ := entities.NewExecution(entities.NewExecutionParams{OSID: "x"}, saga, time.Now())
	if err := repo.Commit(ctx, ports.Write{Execution: e}); !errors.Is(err, errBoom) || e.Version != 0 {
		t.Errorf("Commit: %v (versão %d)", err, e.Version)
	}

	fake.describe = func(*ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error) {
		return &ddb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusCreating}}, nil
	}
	if err := repo.Ping(ctx); err == nil {
		t.Error("Ping deveria falhar com tabela em criação")
	}
	fake.describe = func(*ddb.DescribeTableInput) (*ddb.DescribeTableOutput, error) {
		return &ddb.DescribeTableOutput{}, nil
	}
	if err := repo.Ping(ctx); err == nil {
		t.Error("Ping deveria falhar sem descrição da tabela")
	}
}

func TestRepository_ItensCorrompidos(t *testing.T) {
	ctx := context.Background()
	bad := map[string]typesAV{
		attrPK: str("EXEC#x"), attrSK: str(skState),
		"version": str("não é número"),
	}
	badEvent := map[string]typesAV{
		attrPK: str("EXEC#x"), attrSK: str("EVT#1"),
		"occurred_at": &types.AttributeValueMemberN{Value: "1"},
	}
	fake := &fakeAPI{
		getItem: func(*ddb.GetItemInput) (*ddb.GetItemOutput, error) { return &ddb.GetItemOutput{Item: bad}, nil },
		query: func(in *ddb.QueryInput) (*ddb.QueryOutput, error) {
			if in.IndexName != nil {
				return &ddb.QueryOutput{Items: []map[string]typesAV{bad}}, nil
			}
			return &ddb.QueryOutput{Items: []map[string]typesAV{badEvent}}, nil
		},
	}
	repo := NewRepository(fake, "t")
	if _, err := repo.Get(ctx, "x"); err == nil {
		t.Error("Get deveria falhar com item corrompido")
	}
	if _, _, err := repo.GetWithTimeline(ctx, "x"); err == nil {
		t.Error("GetWithTimeline deveria falhar com evento corrompido")
	}
	if _, err := repo.ListByStatus(ctx, entities.StatusQueuedRepair, 5); err == nil {
		t.Error("ListByStatus deveria falhar com item corrompido")
	}
	fake.query = func(*ddb.QueryInput) (*ddb.QueryOutput, error) {
		return &ddb.QueryOutput{Items: []map[string]typesAV{bad}}, nil
	}
	if _, _, err := repo.GetWithTimeline(ctx, "x"); err == nil {
		t.Error("GetWithTimeline deveria falhar com estado corrompido")
	}
}

func TestRepository_PaginaContagem(t *testing.T) {
	calls := 0
	fake := &fakeAPI{query: func(in *ddb.QueryInput) (*ddb.QueryOutput, error) {
		calls++
		if in.ExclusiveStartKey == nil {
			return &ddb.QueryOutput{Count: 3, LastEvaluatedKey: map[string]typesAV{attrPK: str("p")}}, nil
		}
		return &ddb.QueryOutput{Count: 2}, nil
	}}
	n, err := NewRepository(fake, "t").CountByStatus(context.Background(), entities.StatusInRepair)
	if err != nil || n != 5 || calls != 2 {
		t.Fatalf("n=%d calls=%d err=%v", n, calls, err)
	}
}

func TestRelay_ErrosDaAPI(t *testing.T) {
	ctx := context.Background()
	item := outboxItem{PK: pkOutbox, SK: "1#m", MessageID: "m", MessageType: "x", Body: "{}"}
	av := map[string]typesAV{
		attrPK: str(item.PK), attrSK: str(item.SK), "message_id": str("m"), "message_type": str("x"),
		"body": str(`{"message_id":"m","type":"x"}`), "published": &types.AttributeValueMemberBOOL{Value: false},
		"destination_kind": str("topic"), "destination": str("arn"),
	}
	fake := &fakeAPI{
		query:  func(*ddb.QueryInput) (*ddb.QueryOutput, error) { return nil, errBoom },
		update: func(*ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error) { return nil, errBoom },
	}
	relay := NewRelay(fake, "t", messaging.NewMemoryPublisher(), nil, nil).WithInterval(0)
	if relay.interval != defaultRelayInterval {
		t.Fatal("intervalo inválido mantém o padrão")
	}
	if _, err := relay.Flush(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Flush (query): %v", err)
	}
	if _, err := relay.Pending(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Pending: %v", err)
	}

	fake.query = func(*ddb.QueryInput) (*ddb.QueryOutput, error) {
		return &ddb.QueryOutput{Items: []map[string]typesAV{av}}, nil
	}
	if _, err := relay.Flush(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Flush (claim): %v", err)
	}

	// claim ok, markPublished falha
	updates := 0
	fake.update = func(*ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error) {
		updates++
		if updates == 1 {
			return &ddb.UpdateItemOutput{}, nil
		}
		return nil, errBoom
	}
	if _, err := relay.Flush(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Flush (markPublished): %v", err)
	}

	// claim ok, publish falha, release falha
	updates = 0
	pub := messaging.NewMemoryPublisher()
	pub.Err = errors.New("sns")
	relay = NewRelay(fake, "t", pub, nil, nil)
	if _, err := relay.Flush(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Flush (release): %v", err)
	}

	// erro longo é truncado
	var gotErr string
	fake.update = func(in *ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error) {
		if v, ok := in.ExpressionAttributeValues[":err"].(*types.AttributeValueMemberS); ok {
			gotErr = v.Value
		}
		return &ddb.UpdateItemOutput{}, nil
	}
	long := make([]byte, 2*maxErrorLength)
	for i := range long {
		long[i] = 'x'
	}
	if err := relay.release(ctx, &item, errors.New(string(long))); err != nil || len(gotErr) != maxErrorLength {
		t.Errorf("release: err=%v len=%d", err, len(gotErr))
	}

	// item corrompido na varredura
	fake.query = func(*ddb.QueryInput) (*ddb.QueryOutput, error) {
		return &ddb.QueryOutput{Items: []map[string]typesAV{{"attempts": str("x")}}}, nil
	}
	if _, err := relay.Flush(ctx); err == nil {
		t.Error("Flush deveria falhar com item corrompido")
	}
}

func TestTraceHeaders_SemTransacao(t *testing.T) {
	if h := traceHeaders(context.Background(), true); h != nil {
		t.Fatalf("sem transação não há headers: %v", h)
	}
	if h := traceHeaders(context.Background(), false); h != nil {
		t.Fatal("sem mensagens não captura headers")
	}
}

func TestCommit_EnvelopeInvalido(t *testing.T) {
	repo := NewRepository(&fakeAPI{}, "t")
	bad := ports.OutboxMessage{Envelope: contracts.Envelope{MessageID: "m", Type: "x", Payload: []byte("{")}}
	if err := repo.Commit(context.Background(), ports.Write{Outbox: []ports.OutboxMessage{bad}}); err == nil {
		t.Fatal("esperava erro ao serializar envelope inválido")
	}
}

func TestBackoff(t *testing.T) {
	cases := map[int]time.Duration{0: 0, 1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second, 9: 256 * time.Second, 10: MaxBackoff, 1000: MaxBackoff}
	for attempts, want := range cases {
		if got := Backoff(attempts); got != want {
			t.Errorf("Backoff(%d) = %s, esperava %s", attempts, got, want)
		}
	}
}

func TestRelease_AgendaProximaTentativaEAvisaMensagemTravada(t *testing.T) {
	now := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	var next int64
	fake := &fakeAPI{update: func(in *ddb.UpdateItemInput) (*ddb.UpdateItemOutput, error) {
		if v, ok := in.ExpressionAttributeValues[":next"].(*types.AttributeValueMemberN); ok {
			next, _ = strconv.ParseInt(v.Value, 10, 64)
		}
		return &ddb.UpdateItemOutput{}, nil
	}}
	var buf bytes.Buffer
	relay := NewRelay(fake, "t", messaging.NewMemoryPublisher(), nil, slog.New(slog.NewJSONHandler(&buf, nil)))
	relay.now = func() time.Time { return now }

	item := outboxItem{PK: pkOutbox, SK: "1#m", MessageID: "m", MessageType: "x", Attempts: 2}
	if err := relay.release(context.Background(), &item, errors.New("sns")); err != nil {
		t.Fatal(err)
	}
	if want := now.Add(4 * time.Second).UnixMilli(); next != want {
		t.Fatalf("terceira falha espera 4 s: next=%d, esperava %d", next, want)
	}
	if !strings.Contains(buf.String(), `"msg":"outbox_publish_failed"`) || strings.Contains(buf.String(), "outbox_message_stuck") {
		t.Fatalf("antes do limite é aviso: %s", buf.String())
	}

	buf.Reset()
	item.Attempts = StuckAfter - 1
	if err := relay.release(context.Background(), &item, errors.New("sns")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"msg":"outbox_message_stuck"`) || !strings.Contains(buf.String(), `"level":"ERROR"`) {
		t.Fatalf("no limite vira erro outbox_message_stuck: %s", buf.String())
	}
	if want := now.Add(MaxBackoff).UnixMilli(); next != want {
		t.Fatalf("no limite a espera é a máxima: next=%d, esperava %d", next, want)
	}
}
