// Package dynamodb é o adapter de persistência do execution-service sobre uma tabela única
// (single-table design) do DynamoDB.
//
//	Item                      pk                              sk                    gsi1pk           gsi1sk
//	Execução (estado atual)   EXEC#{os_id}                    STATE                 QUEUE#{status}   {priority}#{enqueued_at}
//	Linha do tempo            EXEC#{os_id}                    EVT#{ts}#{id}         —                —
//	Outbox                    OUTBOX                          {ts}#{message_id}     —                —
//	Inbox (idempotência)      MSG#{consumer}#{message_id}     MSG                   —                —
//
// O gsi1 é esparso: só execuções em status ativo têm gsi1pk, então o índice contém apenas o
// trabalho em andamento na oficina (as finalizadas saem dele sozinhas).
package dynamodb

import (
	"fmt"
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
)

const (
	attrPK     = "pk"
	attrSK     = "sk"
	attrGSI1PK = "gsi1pk"
	attrGSI1SK = "gsi1sk"

	// IndexGSI1 é o índice da fila de trabalho.
	IndexGSI1 = "gsi1"

	skState  = "STATE"
	pkOutbox = "OUTBOX"
	skInbox  = "MSG"

	prefixExec  = "EXEC#"
	prefixEvent = "EVT#"
	prefixQueue = "QUEUE#"
	prefixInbox = "MSG#"

	kindExecution = "execution"
	kindTimeline  = "timeline"
	kindOutbox    = "outbox"
	kindInbox     = "inbox"

	// sortableLayout tem largura fixa (sempre 9 dígitos de fração), então a ordem
	// lexicográfica das chaves é a ordem cronológica.
	sortableLayout = "2006-01-02T15:04:05.000000000Z"
)

func sortable(t time.Time) string { return t.UTC().Format(sortableLayout) }

func execPK(osID string) string { return prefixExec + osID }

func eventSK(at time.Time, id string) string { return prefixEvent + sortable(at) + "#" + id }

func queuePK(s entities.Status) string { return prefixQueue + string(s) }

func queueSK(priority int, enqueuedAt time.Time) string {
	return fmt.Sprintf("%d#%s", priority, sortable(enqueuedAt))
}

func outboxSK(at time.Time, messageID string) string { return sortable(at) + "#" + messageID }

func inboxPK(consumer, messageID string) string { return prefixInbox + consumer + "#" + messageID }
