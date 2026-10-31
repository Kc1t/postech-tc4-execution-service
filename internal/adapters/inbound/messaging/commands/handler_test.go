package commands

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports/mocks"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/contracts"
	"github.com/Kc1t/postech-tc4-execution-service/pkg/messaging"
	"go.uber.org/mock/gomock"
)

type fixture struct {
	sd *mocks.MockStartDiagnosisUseCase
	sr *mocks.MockStartRepairUseCase
	ce *mocks.MockCancelExecutionUseCase
	h  *Handler
}

func newFixture(t *testing.T) fixture {
	ctrl := gomock.NewController(t)
	f := fixture{
		sd: mocks.NewMockStartDiagnosisUseCase(ctrl),
		sr: mocks.NewMockStartRepairUseCase(ctrl),
		ce: mocks.NewMockCancelExecutionUseCase(ctrl),
	}
	f.h = NewHandler(f.sd, f.sr, f.ce)
	return f
}

func envelope(t *testing.T, msgType string, payload any) contracts.Envelope {
	t.Helper()
	env, err := contracts.NewEnvelope(msgType, "os-service", contracts.Meta{SagaID: "saga-1", OSID: "os-1", CorrelationID: "corr-1"}, payload)
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func wantMeta(env contracts.Envelope) ports.MessageMeta {
	return ports.MessageMeta{
		MessageID: env.MessageID, Type: env.Type, SagaID: "saga-1", OSID: "os-1",
		CorrelationID: "corr-1", Producer: "os-service",
	}
}

type registry map[string]messaging.Handler

func (r registry) Handle(t string, h messaging.Handler) { r[t] = h }

func TestRegister(t *testing.T) {
	f := newFixture(t)
	reg := registry{}
	f.h.Register(reg)
	for _, typ := range []string{contracts.CmdStartDiagnosis, contracts.CmdStartRepair, contracts.CmdCancelExecution} {
		if reg[typ] == nil {
			t.Errorf("sem handler para %s", typ)
		}
	}
}

func TestStartDiagnosis(t *testing.T) {
	f := newFixture(t)
	env := envelope(t, contracts.CmdStartDiagnosis, contracts.StartDiagnosis{
		OSCode: 42, Vehicle: contracts.Vehicle{Plate: "ABC1D23", Brand: "Fiat", Model: "Uno", Year: 2020}, Notes: "freio",
	})
	f.sd.EXPECT().Execute(gomock.Any(), ports.StartDiagnosisInput{
		Message: wantMeta(env), OSCode: 42,
		Vehicle: entities.Vehicle{Plate: "ABC1D23", Brand: "Fiat", Model: "Uno", Year: 2020}, Notes: "freio",
	}).Return(nil)
	if err := f.h.StartDiagnosis(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestStartRepair(t *testing.T) {
	f := newFixture(t)
	env := envelope(t, contracts.CmdStartRepair, contracts.StartRepair{
		OSCode:          42,
		Items:           []contracts.Item{{Kind: "part", Ref: "P-1", Description: "Pastilha", Quantity: 2, UnitPrice: 80}},
		SimulateFailure: contracts.FailRepair,
	})
	f.sr.EXPECT().Execute(gomock.Any(), ports.StartRepairInput{
		Message:         wantMeta(env),
		Items:           []entities.RepairItem{{Kind: "part", Ref: "P-1", Description: "Pastilha", Quantity: 2, UnitPrice: 80}},
		SimulateFailure: contracts.FailRepair,
	}).Return(nil)
	if err := f.h.StartRepair(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestCancelExecution(t *testing.T) {
	f := newFixture(t)
	env := envelope(t, contracts.CmdCancelExecution, contracts.CancelExecution{Reason: "pagamento recusado"})
	f.ce.EXPECT().Execute(gomock.Any(), ports.CancelExecutionInput{Message: wantMeta(env), Reason: "pagamento recusado"}).Return(nil)
	if err := f.h.CancelExecution(context.Background(), env); err != nil {
		t.Fatal(err)
	}
}

func TestErroTransitorioVoltaParaAFila(t *testing.T) {
	f := newFixture(t)
	boom := errors.New("dynamodb fora")
	f.ce.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(boom)
	if err := f.h.CancelExecution(context.Background(), envelope(t, contracts.CmdCancelExecution, contracts.CancelExecution{})); !errors.Is(err, boom) {
		t.Fatalf("esperava o erro do caso de uso, veio %v", err)
	}
}

func TestMensagensMalformadas(t *testing.T) {
	f := newFixture(t)
	semOS := envelope(t, contracts.CmdStartDiagnosis, contracts.StartDiagnosis{})
	semOS.OSID = ""
	if err := f.h.StartDiagnosis(context.Background(), semOS); !errors.Is(err, ErrMissingOSID) {
		t.Fatalf("esperava ErrMissingOSID, veio %v", err)
	}

	// Payload com tipos incompatíveis com os três contratos.
	bad := envelope(t, contracts.CmdStartRepair, nil)
	bad.Payload = json.RawMessage(`{"os_code":"x","items":"y","reason":1}`)
	for name, fn := range map[string]func(context.Context, contracts.Envelope) error{
		"StartDiagnosis":  f.h.StartDiagnosis,
		"StartRepair":     f.h.StartRepair,
		"CancelExecution": f.h.CancelExecution,
	} {
		if err := fn(context.Background(), bad); err == nil {
			t.Errorf("%s: payload inválido deveria falhar", name)
		}
	}
}
