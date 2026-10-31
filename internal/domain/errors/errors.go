// Package domainerrors reúne os erros sentinela do domínio de execução. Os adapters traduzem
// cada um para o seu protocolo (código HTTP, ack/retry da fila) com errors.Is.
package domainerrors

import "errors"

var (
	// ErrNotFound indica que não existe execução para a OS informada.
	ErrNotFound = errors.New("execução não encontrada")
	// ErrAlreadyExists indica que já existe execução para a OS (criação concorrente ou repetida).
	ErrAlreadyExists = errors.New("execução já existe para esta OS")
	// ErrInvalidTransition indica uma mudança de status não permitida pela máquina de estados.
	ErrInvalidTransition = errors.New("transição de status inválida")
	// ErrConcurrentUpdate indica que a versão gravada mudou entre a leitura e a escrita
	// (optimistic lock). Quem chama pode reler e tentar de novo.
	ErrConcurrentUpdate = errors.New("a execução foi alterada por outra operação; releia e tente novamente")
	// ErrDuplicateMessage indica que a mensagem já foi processada por este consumidor (inbox).
	ErrDuplicateMessage = errors.New("mensagem já processada")
	// ErrInvalidDiagnosis indica um diagnóstico sem itens ou com itens inválidos.
	ErrInvalidDiagnosis = errors.New("diagnóstico inválido")
	// ErrInvalidExecution indica dados obrigatórios ausentes ao criar a execução.
	ErrInvalidExecution = errors.New("dados da execução inválidos")
	// ErrInvalidStatus indica um status desconhecido ou não permitido no filtro.
	ErrInvalidStatus = errors.New("status inválido")
)
