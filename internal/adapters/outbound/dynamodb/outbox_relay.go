package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"
)

const (
	defaultRelayInterval = 500 * time.Millisecond
	defaultRelayBatch    = 25
	// defaultLease é por quanto tempo uma réplica reserva a mensagem para publicá-la. Se a réplica
	// cair no meio, outra assume depois do prazo.
	defaultLease = 30 * time.Second
	// PublishedRetention é por quanto tempo a mensagem publicada fica na partição OUTBOX antes
	// do TTL removê-la (serve para auditoria e mantém a varredura curta).
	PublishedRetention = 24 * time.Hour
	maxErrorLength     = 500
)

// Relay publica as mensagens pendentes do outbox. Mesma semântica do pkg/outbox (Postgres):
// entrega "pelo menos uma vez", em ordem de gravação, com o contexto de trace capturado na
// gravação reaceito numa transação de background do New Relic antes de publicar.
//
// No lugar do FOR UPDATE SKIP LOCKED, cada mensagem é reservada com um UpdateItem condicional
// (lease): várias réplicas podem rodar o relay sem publicar a mesma mensagem em paralelo.
type Relay struct {
	api      API
	table    string
	pub      messaging.Publisher
	nr       *newrelic.Application
	log      *slog.Logger
	interval time.Duration
	batch    int
	lease    time.Duration
	owner    string
	now      func() time.Time
}

func NewRelay(api API, table string, pub messaging.Publisher, nr *newrelic.Application, log *slog.Logger) *Relay {
	if log == nil {
		log = slog.Default()
	}
	host, _ := os.Hostname()
	return &Relay{
		api:      api,
		table:    table,
		pub:      pub,
		nr:       nr,
		log:      log.With("component", "outbox_relay"),
		interval: defaultRelayInterval,
		batch:    defaultRelayBatch,
		lease:    defaultLease,
		owner:    host + "/" + uuid.NewString()[:8],
		now:      func() time.Time { return time.Now().UTC() },
	}
}

// WithInterval ajusta o intervalo entre varreduras.
func (r *Relay) WithInterval(d time.Duration) *Relay {
	if d > 0 {
		r.interval = d
	}
	return r
}

// Run varre o outbox até ctx ser cancelado.
func (r *Relay) Run(ctx context.Context) {
	r.log.Info("outbox_relay_started", "interval_ms", r.interval.Milliseconds(), "owner", r.owner)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			r.log.Info("outbox_relay_stopped")
			return
		case <-t.C:
			if _, err := r.Flush(ctx); err != nil && ctx.Err() == nil {
				r.log.Error("outbox_flush_failed", "error", err)
			}
		}
	}
}

// Flush publica um lote de mensagens pendentes e devolve quantas foram publicadas.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	pending, err := r.pending(ctx, r.batch)
	if err != nil {
		return 0, err
	}
	published := 0
	for i := range pending {
		m := &pending[i]
		claimed, err := r.claim(ctx, m)
		if err != nil {
			return published, err
		}
		if !claimed {
			continue // outra réplica reservou ou já publicou
		}
		if err := r.publish(ctx, m); err != nil {
			r.log.Warn("outbox_publish_failed", "message_id", m.MessageID, "message_type", m.MessageType,
				"saga_id", m.SagaID, "os_id", m.OSID, "attempts", m.Attempts+1, "error", err)
			if rerr := r.release(ctx, m, err); rerr != nil {
				return published, rerr
			}
			continue
		}
		if err := r.markPublished(ctx, m); err != nil {
			return published, err
		}
		published++
	}
	return published, nil
}

// Pending conta as mensagens ainda não publicadas (health e testes).
func (r *Relay) Pending(ctx context.Context) (int, error) {
	in := r.pendingQuery(false)
	in.Select = types.SelectCount
	total := 0
	for {
		out, err := r.api.Query(ctx, in)
		if err != nil {
			return 0, fmt.Errorf("outbox: contar pendentes: %w", err)
		}
		total += int(out.Count)
		if len(out.LastEvaluatedKey) == 0 {
			return total, nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (r *Relay) pendingQuery(skipLeased bool) *ddb.QueryInput {
	filter := "#pub = :false"
	names := map[string]string{"#pk": attrPK, "#pub": "published"}
	values := map[string]types.AttributeValue{
		":pk":    str(pkOutbox),
		":false": &types.AttributeValueMemberBOOL{Value: false},
	}
	if skipLeased {
		filter += " AND (attribute_not_exists(#lease) OR #lease < :now)"
		names["#lease"] = "lease_until"
		values[":now"] = num(r.now().UnixMilli())
	}
	return &ddb.QueryInput{
		TableName:                 aws.String(r.table),
		KeyConditionExpression:    aws.String("#pk = :pk"),
		FilterExpression:          aws.String(filter),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ConsistentRead:            aws.Bool(true),
		ScanIndexForward:          aws.Bool(true),
	}
}

// pending varre a partição OUTBOX em ordem de gravação e devolve até n mensagens livres.
func (r *Relay) pending(ctx context.Context, n int) ([]outboxItem, error) {
	var out []outboxItem
	err := paginate(ctx, r.api, r.pendingQuery(true), func(item map[string]types.AttributeValue) (bool, error) {
		var m outboxItem
		if err := attributevalue.UnmarshalMap(item, &m); err != nil {
			return false, err
		}
		out = append(out, m)
		return len(out) < n, nil
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: varrer pendentes: %w", err)
	}
	return out, nil
}

func (r *Relay) claim(ctx context.Context, m *outboxItem) (bool, error) {
	now := r.now()
	_, err := r.api.UpdateItem(ctx, &ddb.UpdateItemInput{
		TableName:           aws.String(r.table),
		Key:                 key(m.PK, m.SK),
		UpdateExpression:    aws.String("SET #lease = :until, #owner = :me"),
		ConditionExpression: aws.String("#pub = :false AND (attribute_not_exists(#lease) OR #lease < :now)"),
		ExpressionAttributeNames: map[string]string{
			"#lease": "lease_until", "#owner": "lease_owner", "#pub": "published",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":until": num(now.Add(r.lease).UnixMilli()),
			":me":    str(r.owner),
			":false": &types.AttributeValueMemberBOOL{Value: false},
			":now":   num(now.UnixMilli()),
		},
	})
	var ccf *types.ConditionalCheckFailedException
	if errors.As(err, &ccf) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("outbox: reservar %s: %w", m.MessageID, err)
	}
	return true, nil
}

func (r *Relay) publish(ctx context.Context, m *outboxItem) error {
	env, err := contracts.Parse([]byte(m.Body))
	if err != nil {
		return err
	}
	if r.nr != nil {
		txn := r.nr.StartTransaction("OutboxRelay/" + m.MessageType)
		defer txn.End()
		h := http.Header{}
		for k, v := range m.TraceHeaders {
			h.Set(k, v)
		}
		txn.AcceptDistributedTraceHeaders(newrelic.TransportOther, h)
		txn.AddAttribute("message_id", env.MessageID)
		txn.AddAttribute("saga_id", env.SagaID)
		txn.AddAttribute("os_id", env.OSID)
		ctx = newrelic.NewContext(ctx, txn)
	}
	return r.pub.Publish(ctx, messaging.Destination{Kind: messaging.Kind(m.DestinationKind), Name: m.Destination}, env)
}

func (r *Relay) markPublished(ctx context.Context, m *outboxItem) error {
	now := r.now()
	_, err := r.api.UpdateItem(ctx, &ddb.UpdateItemInput{
		TableName:        aws.String(r.table),
		Key:              key(m.PK, m.SK),
		UpdateExpression: aws.String("SET #pub = :true, #pat = :at, #ttl = :ttl REMOVE #lease, #owner, #lerr"),
		ExpressionAttributeNames: map[string]string{
			"#pub": "published", "#pat": "published_at", "#ttl": "ttl",
			"#lease": "lease_until", "#owner": "lease_owner", "#lerr": "last_error",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":true": &types.AttributeValueMemberBOOL{Value: true},
			":at":   str(now.Format(time.RFC3339Nano)),
			":ttl":  num(now.Add(PublishedRetention).Unix()),
		},
	})
	if err != nil {
		return fmt.Errorf("outbox: marcar %s como publicada: %w", m.MessageID, err)
	}
	return nil
}

func (r *Relay) release(ctx context.Context, m *outboxItem, cause error) error {
	msg := cause.Error()
	if len(msg) > maxErrorLength {
		msg = msg[:maxErrorLength]
	}
	_, err := r.api.UpdateItem(ctx, &ddb.UpdateItemInput{
		TableName:        aws.String(r.table),
		Key:              key(m.PK, m.SK),
		UpdateExpression: aws.String("SET #att = if_not_exists(#att, :zero) + :one, #lerr = :err REMOVE #lease, #owner"),
		ExpressionAttributeNames: map[string]string{
			"#att": "attempts", "#lerr": "last_error", "#lease": "lease_until", "#owner": "lease_owner",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":zero": num(0), ":one": num(1), ":err": str(msg),
		},
	})
	if err != nil {
		return fmt.Errorf("outbox: registrar falha de %s: %w", m.MessageID, err)
	}
	return nil
}

func num(n int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}
