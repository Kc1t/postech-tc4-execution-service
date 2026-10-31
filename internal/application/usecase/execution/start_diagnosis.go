package executionuc

import (
	"context"
	"errors"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/logger"
)

// StartDiagnosis trata o comando execution.StartDiagnosis: abre a execução na fila de
// diagnóstico. É idempotente por os_id; nenhum evento sai agora — o DiagnosisStarted é
// publicado quando o mecânico pega o veículo.
type StartDiagnosis struct{ base }

func NewStartDiagnosis(d Deps) *StartDiagnosis { return &StartDiagnosis{newBase(d)} }

func (uc *StartDiagnosis) Execute(ctx context.Context, in ports.StartDiagnosisInput) error {
	return uc.once(ctx, in.Message, func() error {
		log := logger.FromContext(ctx)
		existing, err := uc.repo.Get(ctx, in.Message.OSID)
		switch {
		case err == nil:
			log.Info("command_ignored", "reason", "execução já existe para a OS", "status", string(existing.Status))
			return nil
		case !errors.Is(err, domainerrors.ErrNotFound):
			return err
		}

		exec, err := entities.NewExecution(entities.NewExecutionParams{
			OSID:    in.Message.OSID,
			OSCode:  in.OSCode,
			SagaID:  in.Message.SagaID,
			Vehicle: in.Vehicle,
			Notes:   in.Notes,
		}, entities.ServiceActor(in.Message.Producer), uc.now())
		if err != nil {
			return err
		}

		err = uc.repo.Commit(ctx, ports.Write{Execution: exec, Inbox: inbox(in.Message)})
		if errors.Is(err, domainerrors.ErrAlreadyExists) {
			log.Info("command_ignored", "reason", "execução criada por entrega concorrente")
			return nil
		}
		if err != nil {
			return err
		}
		log.Info("execution_status_changed", "from", "", "to", string(exec.Status), "os_code", exec.OSCode)
		return nil
	})
}
