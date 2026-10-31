package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"
)

// API é o subconjunto do cliente DynamoDB usado pelo adapter (o *dynamodb.Client satisfaz).
type API interface {
	GetItem(ctx context.Context, in *ddb.GetItemInput, optFns ...func(*ddb.Options)) (*ddb.GetItemOutput, error)
	Query(ctx context.Context, in *ddb.QueryInput, optFns ...func(*ddb.Options)) (*ddb.QueryOutput, error)
	TransactWriteItems(ctx context.Context, in *ddb.TransactWriteItemsInput, optFns ...func(*ddb.Options)) (*ddb.TransactWriteItemsOutput, error)
	UpdateItem(ctx context.Context, in *ddb.UpdateItemInput, optFns ...func(*ddb.Options)) (*ddb.UpdateItemOutput, error)
	DescribeTable(ctx context.Context, in *ddb.DescribeTableInput, optFns ...func(*ddb.Options)) (*ddb.DescribeTableOutput, error)
}

// InboxRetention é por quanto tempo o registro de inbox é mantido (TTL). É maior que a
// retenção máxima do SQS (14 dias), então nenhuma reentrega encontra o registro expirado.
const InboxRetention = 15 * 24 * time.Hour

// Repository implementa ports.ExecutionRepository.
type Repository struct {
	api   API
	table string
	now   func() time.Time
}

var _ ports.ExecutionRepository = (*Repository)(nil)

func NewRepository(api API, table string) *Repository {
	return &Repository{api: api, table: table, now: func() time.Time { return time.Now().UTC() }}
}

// Table devolve o nome da tabela.
func (r *Repository) Table() string { return r.table }

// Ping verifica se a tabela existe e está ativa (readiness).
func (r *Repository) Ping(ctx context.Context) error {
	out, err := r.api.DescribeTable(ctx, &ddb.DescribeTableInput{TableName: aws.String(r.table)})
	if err != nil {
		return fmt.Errorf("dynamodb: describe table %s: %w", r.table, err)
	}
	if out.Table == nil || out.Table.TableStatus != types.TableStatusActive {
		status := types.TableStatus("desconhecido")
		if out.Table != nil {
			status = out.Table.TableStatus
		}
		return fmt.Errorf("dynamodb: tabela %s não está ativa (%s)", r.table, status)
	}
	return nil
}

// txItemRole diz o papel de cada item da transação, para interpretar os motivos de cancelamento.
type txItemRole int

const (
	roleState txItemRole = iota
	roleTimeline
	roleOutbox
	roleInbox
)

// Commit grava estado, linha do tempo, outbox e inbox numa única TransactWriteItems.
//
//   - STATE: Put com attribute_not_exists(pk) na criação ou version = :esperada na atualização;
//   - EVT# e OUTBOX: Put de itens novos (chaves únicas por id);
//   - MSG# (inbox): Put com attribute_not_exists(pk).
//
// Se qualquer condição falhar, nada é gravado: não existe estado sem evento nem evento sem estado.
func (r *Repository) Commit(ctx context.Context, w ports.Write) error {
	var (
		items    []types.TransactWriteItem
		roles    []txItemRole
		creating bool
		version  int
	)
	add := func(role txItemRole, v any, cond string, names map[string]string, values map[string]types.AttributeValue) error {
		av, err := attributevalue.MarshalMap(v)
		if err != nil {
			return fmt.Errorf("dynamodb: serializar item: %w", err)
		}
		put := &types.Put{TableName: aws.String(r.table), Item: av}
		if cond != "" {
			put.ConditionExpression = aws.String(cond)
			put.ExpressionAttributeNames = names
			put.ExpressionAttributeValues = values
		}
		items = append(items, types.TransactWriteItem{Put: put})
		roles = append(roles, role)
		return nil
	}
	notExists := func() (string, map[string]string) {
		return "attribute_not_exists(#pk)", map[string]string{"#pk": attrPK}
	}

	now := r.now()
	if e := w.Execution; e != nil {
		creating = e.IsNew()
		version = e.Version + 1
		var err error
		if creating {
			cond, names := notExists()
			err = add(roleState, toStateItem(e, version), cond, names, nil)
		} else {
			err = add(roleState, toStateItem(e, version), "#v = :expected",
				map[string]string{"#v": "version"},
				map[string]types.AttributeValue{":expected": &types.AttributeValueMemberN{Value: fmt.Sprint(e.Version)}})
		}
		if err != nil {
			return err
		}
		for _, ev := range e.PendingEvents() {
			if err := add(roleTimeline, toEventItem(e.OSID, ev), "", nil, nil); err != nil {
				return err
			}
		}
	}
	headers := traceHeaders(ctx, len(w.Outbox) > 0)
	for _, m := range w.Outbox {
		body, err := m.Envelope.Marshal()
		if err != nil {
			return fmt.Errorf("dynamodb: serializar envelope %s: %w", m.Envelope.Type, err)
		}
		if err := add(roleOutbox, outboxItem{
			PK:              pkOutbox,
			SK:              outboxSK(now, m.Envelope.MessageID),
			Kind:            kindOutbox,
			MessageID:       m.Envelope.MessageID,
			MessageType:     m.Envelope.Type,
			SagaID:          m.Envelope.SagaID,
			OSID:            m.Envelope.OSID,
			DestinationKind: string(m.Destination.Kind),
			Destination:     m.Destination.Name,
			Body:            string(body),
			TraceHeaders:    headers,
			CreatedAt:       now,
		}, "", nil, nil); err != nil {
			return err
		}
	}
	if in := w.Inbox; in != nil {
		cond, names := notExists()
		if err := add(roleInbox, inboxItem{
			PK:          inboxPK(in.Consumer, in.MessageID),
			SK:          skInbox,
			Kind:        kindInbox,
			Consumer:    in.Consumer,
			MessageID:   in.MessageID,
			MessageType: in.Type,
			ProcessedAt: now,
			TTL:         now.Add(InboxRetention).Unix(),
		}, cond, names, nil); err != nil {
			return err
		}
	}
	if len(items) == 0 {
		return nil
	}

	_, err := r.api.TransactWriteItems(ctx, &ddb.TransactWriteItemsInput{
		TransactItems: items,
		// Token idempotente: um retry automático do SDK depois de timeout não reaplica a
		// transação nem esbarra na própria condição de versão.
		ClientRequestToken: aws.String(uuid.NewString()),
	})
	if err != nil {
		osID := ""
		if w.Execution != nil {
			osID = w.Execution.OSID
		}
		return mapTxError(err, roles, creating, osID)
	}
	if e := w.Execution; e != nil {
		e.Version = version
		e.ClearPendingEvents()
	}
	return nil
}

// traceHeaders captura o contexto de distributed tracing da transação New Relic em ctx, para o
// relay continuar o mesmo trace ao publicar (mesma lógica do pkg/outbox.Enqueue).
func traceHeaders(ctx context.Context, needed bool) map[string]string {
	if !needed {
		return nil
	}
	txn := newrelic.FromContext(ctx)
	if txn == nil {
		return nil
	}
	h := http.Header{}
	txn.InsertDistributedTraceHeaders(h)
	out := make(map[string]string, len(h))
	for k := range h {
		out[k] = h.Get(k)
	}
	return out
}

// mapTxError traduz o cancelamento da transação para os erros do domínio, usando o motivo de
// cada item (na mesma ordem dos itens enviados).
func mapTxError(err error, roles []txItemRole, creating bool, osID string) error {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		var conflict *types.TransactionConflictException
		var inProgress *types.TransactionInProgressException
		if errors.As(err, &conflict) || errors.As(err, &inProgress) {
			return fmt.Errorf("%w: os_id=%s: %v", domainerrors.ErrConcurrentUpdate, osID, err)
		}
		return fmt.Errorf("dynamodb: transact write: %w", err)
	}
	codes := cancellationCodes(tce, len(roles))
	for i, code := range codes {
		if roles[i] == roleInbox && code == "ConditionalCheckFailed" {
			return domainerrors.ErrDuplicateMessage
		}
	}
	for i, code := range codes {
		if roles[i] == roleState && code == "ConditionalCheckFailed" {
			if creating {
				return fmt.Errorf("%w: os_id=%s", domainerrors.ErrAlreadyExists, osID)
			}
			return fmt.Errorf("%w: os_id=%s", domainerrors.ErrConcurrentUpdate, osID)
		}
	}
	for _, code := range codes {
		if code == "TransactionConflict" {
			return fmt.Errorf("%w: os_id=%s: transação concorrente", domainerrors.ErrConcurrentUpdate, osID)
		}
	}
	return fmt.Errorf("dynamodb: transação cancelada: %w", err)
}

// cancellationCodes devolve o código de cancelamento de cada item. Usa CancellationReasons e,
// se o endpoint não as devolver, a lista "[A, B]" do final da mensagem de erro.
func cancellationCodes(tce *types.TransactionCanceledException, n int) []string {
	codes := make([]string, n)
	if len(tce.CancellationReasons) > 0 {
		for i, r := range tce.CancellationReasons {
			if i < n {
				codes[i] = aws.ToString(r.Code)
			}
		}
		return codes
	}
	msg := tce.ErrorMessage()
	start, end := strings.LastIndex(msg, "["), strings.LastIndex(msg, "]")
	if start < 0 || end < start {
		return codes
	}
	for i, c := range strings.Split(msg[start+1:end], ",") {
		if i < n {
			codes[i] = strings.TrimSpace(c)
		}
	}
	return codes
}

// Get lê o item STATE com leitura fortemente consistente.
func (r *Repository) Get(ctx context.Context, osID string) (*entities.Execution, error) {
	out, err := r.api.GetItem(ctx, &ddb.GetItemInput{
		TableName:      aws.String(r.table),
		Key:            key(execPK(osID), skState),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: get %s: %w", osID, err)
	}
	if len(out.Item) == 0 {
		return nil, fmt.Errorf("%w: os_id=%s", domainerrors.ErrNotFound, osID)
	}
	var it stateItem
	if err := attributevalue.UnmarshalMap(out.Item, &it); err != nil {
		return nil, fmt.Errorf("dynamodb: ler estado de %s: %w", osID, err)
	}
	return it.toEntity(), nil
}

// GetWithTimeline lê a coleção EXEC#{os_id} inteira numa só Query: os itens EVT# vêm em ordem
// cronológica pela sort key e o STATE ("S" > "E") vem por último.
func (r *Repository) GetWithTimeline(ctx context.Context, osID string) (*entities.Execution, []entities.TimelineEvent, error) {
	var (
		exec     *entities.Execution
		timeline []entities.TimelineEvent
	)
	err := r.query(ctx, &ddb.QueryInput{
		TableName:                 aws.String(r.table),
		KeyConditionExpression:    aws.String("#pk = :pk"),
		ExpressionAttributeNames:  map[string]string{"#pk": attrPK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": str(execPK(osID))},
		ConsistentRead:            aws.Bool(true),
	}, func(item map[string]types.AttributeValue) (bool, error) {
		sk := attrString(item, attrSK)
		switch {
		case sk == skState:
			var it stateItem
			if err := attributevalue.UnmarshalMap(item, &it); err != nil {
				return false, err
			}
			exec = it.toEntity()
		case strings.HasPrefix(sk, prefixEvent):
			var it eventItem
			if err := attributevalue.UnmarshalMap(item, &it); err != nil {
				return false, err
			}
			timeline = append(timeline, it.toEntity())
		}
		return true, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dynamodb: ler execução %s: %w", osID, err)
	}
	if exec == nil {
		return nil, nil, fmt.Errorf("%w: os_id=%s", domainerrors.ErrNotFound, osID)
	}
	return exec, timeline, nil
}

// ListByStatus consulta o gsi1 na partição QUEUE#{status}, em ordem de prioridade e chegada.
func (r *Repository) ListByStatus(ctx context.Context, status entities.Status, limit int) ([]*entities.Execution, error) {
	out := make([]*entities.Execution, 0)
	if limit <= 0 {
		return out, nil
	}
	err := r.query(ctx, &ddb.QueryInput{
		TableName:                 aws.String(r.table),
		IndexName:                 aws.String(IndexGSI1),
		KeyConditionExpression:    aws.String("#gpk = :gpk"),
		ExpressionAttributeNames:  map[string]string{"#gpk": attrGSI1PK},
		ExpressionAttributeValues: map[string]types.AttributeValue{":gpk": str(queuePK(status))},
		ScanIndexForward:          aws.Bool(true),
		Limit:                     aws.Int32(int32(min(limit, 1000))), //nolint:gosec // limitado a 1000
	}, func(item map[string]types.AttributeValue) (bool, error) {
		var it stateItem
		if err := attributevalue.UnmarshalMap(item, &it); err != nil {
			return false, err
		}
		out = append(out, it.toEntity())
		return len(out) < limit, nil
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: listar %s: %w", status, err)
	}
	return out, nil
}

// CountByStatus conta, no gsi1, as execuções de cada status (Select COUNT, sem ler atributos).
func (r *Repository) CountByStatus(ctx context.Context, statuses ...entities.Status) (int, error) {
	total := 0
	for _, s := range statuses {
		in := &ddb.QueryInput{
			TableName:                 aws.String(r.table),
			IndexName:                 aws.String(IndexGSI1),
			KeyConditionExpression:    aws.String("#gpk = :gpk"),
			ExpressionAttributeNames:  map[string]string{"#gpk": attrGSI1PK},
			ExpressionAttributeValues: map[string]types.AttributeValue{":gpk": str(queuePK(s))},
			Select:                    types.SelectCount,
		}
		for {
			out, err := r.api.Query(ctx, in)
			if err != nil {
				return 0, fmt.Errorf("dynamodb: contar %s: %w", s, err)
			}
			total += int(out.Count)
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			in.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}
	return total, nil
}

// IsProcessed consulta o item de inbox da mensagem.
func (r *Repository) IsProcessed(ctx context.Context, consumer, messageID string) (bool, error) {
	out, err := r.api.GetItem(ctx, &ddb.GetItemInput{
		TableName:                aws.String(r.table),
		Key:                      key(inboxPK(consumer, messageID), skInbox),
		ConsistentRead:           aws.Bool(true),
		ProjectionExpression:     aws.String("#pk"),
		ExpressionAttributeNames: map[string]string{"#pk": attrPK},
	})
	if err != nil {
		return false, fmt.Errorf("dynamodb: inbox %s: %w", messageID, err)
	}
	return len(out.Item) > 0, nil
}

// query pagina a consulta e chama fn para cada item até fn devolver false.
func (r *Repository) query(ctx context.Context, in *ddb.QueryInput, fn func(map[string]types.AttributeValue) (bool, error)) error {
	return paginate(ctx, r.api, in, fn)
}

func paginate(ctx context.Context, api API, in *ddb.QueryInput, fn func(map[string]types.AttributeValue) (bool, error)) error {
	for {
		out, err := api.Query(ctx, in)
		if err != nil {
			return err
		}
		for _, item := range out.Items {
			more, err := fn(item)
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func key(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{attrPK: str(pk), attrSK: str(sk)}
}

func str(s string) types.AttributeValue { return &types.AttributeValueMemberS{Value: s} }

func attrString(item map[string]types.AttributeValue, name string) string {
	if v, ok := item[name].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}
