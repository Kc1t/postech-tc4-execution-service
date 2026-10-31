package ports

import (
	"context"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
)

//go:generate mockgen -source=./repositories.go -destination=./mocks/repositories.go -package=mocks

// OutboxMessage é uma mensagem a publicar depois que a mudança de estado for gravada.
type OutboxMessage struct {
	Destination messaging.Destination
	Envelope    contracts.Envelope
}

// InboxEntry identifica a mensagem consumida que provocou a mudança (idempotência).
type InboxEntry struct {
	Consumer  string
	MessageID string
	Type      string
}

// Write é a unidade de trabalho gravada de forma atômica: o novo estado da execução (com os
// eventos de linha do tempo pendentes), as mensagens do outbox e o registro de inbox.
//
// Execution nil grava só mensagens (ex.: recusa de um comando para uma OS desconhecida).
type Write struct {
	Execution *entities.Execution
	Outbox    []OutboxMessage
	Inbox     *InboxEntry
}

// ExecutionRepository persiste o agregado Execution.
type ExecutionRepository interface {
	// Commit grava a unidade de trabalho numa única transação. Erros:
	//   - ErrAlreadyExists: execução nova (Version 0) e já existe uma para a OS;
	//   - ErrConcurrentUpdate: a versão gravada não é mais Execution.Version;
	//   - ErrDuplicateMessage: o Inbox já tinha sido registrado (nada é gravado).
	// Em caso de sucesso incrementa Execution.Version e limpa os eventos pendentes.
	Commit(ctx context.Context, w Write) error
	// Get lê o estado atual (leitura fortemente consistente). ErrNotFound se não existir.
	Get(ctx context.Context, osID string) (*entities.Execution, error)
	// GetWithTimeline lê o estado e a linha do tempo em ordem cronológica.
	GetWithTimeline(ctx context.Context, osID string) (*entities.Execution, []entities.TimelineEvent, error)
	// ListByStatus lista execuções de um status ativo por prioridade e ordem de chegada.
	ListByStatus(ctx context.Context, status entities.Status, limit int) ([]*entities.Execution, error)
	// CountByStatus soma as execuções nos status informados.
	CountByStatus(ctx context.Context, statuses ...entities.Status) (int, error)
	// IsProcessed diz se a mensagem já foi processada pelo consumidor.
	IsProcessed(ctx context.Context, consumer, messageID string) (bool, error)
}
