package entities

import (
	"errors"
	"testing"
	"time"

	domainerrors "github.com/Kc1t/postech-tc4-execution-service/internal/domain/errors"
)

var (
	t0       = time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	saga     = ServiceActor("os-service")
	mechanic = Actor{ID: "user-1", Type: ActorMechanic}
	diag     = Diagnosis{
		Services: []DiagnosedService{{Code: 10}},
		Parts:    []DiagnosedPart{{ManufacturerCode: "BOSCH-123", Quantity: 2}},
		Notes:    "pastilhas gastas",
	}
)

func newExec(t *testing.T) *Execution {
	t.Helper()
	e, err := NewExecution(NewExecutionParams{
		OSID: "os-1", OSCode: 42, SagaID: "saga-1",
		Vehicle: Vehicle{Plate: "ABC1D23", Brand: "Fiat", Model: "Uno", Year: 2020},
		Notes:   "barulho no freio",
	}, saga, t0)
	if err != nil {
		t.Fatalf("NewExecution: %v", err)
	}
	return e
}

// atStatus leva uma execução nova até o status pedido pelo caminho feliz.
func atStatus(t *testing.T, s Status) *Execution {
	t.Helper()
	e := newExec(t)
	steps := []struct {
		reach Status
		do    func() error
	}{
		{StatusInDiagnosis, func() error { return e.BeginDiagnosis(mechanic, t0.Add(time.Minute)) }},
		{StatusDiagnosed, func() error { return e.CompleteDiagnosis(diag, mechanic, t0.Add(2*time.Minute)) }},
		{StatusQueuedRepair, func() error {
			return e.EnqueueRepair([]RepairItem{{Kind: "service", Ref: "10", Quantity: 1, UnitPrice: 100}}, saga, t0.Add(3*time.Minute))
		}},
		{StatusInRepair, func() error { return e.BeginRepair(mechanic, t0.Add(4*time.Minute)) }},
		{StatusFinished, func() error { return e.FinishRepair(mechanic, t0.Add(5*time.Minute)) }},
	}
	switch s {
	case StatusQueuedDiagnosis:
		return e
	case StatusRejected:
		_ = steps[0].do()
		_ = steps[1].do()
		if err := e.RejectRepair("sem vaga", saga, t0.Add(3*time.Minute)); err != nil {
			t.Fatal(err)
		}
		return e
	case StatusCancelled:
		if err := e.Cancel("saga compensada", saga, t0.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		return e
	}
	for _, st := range steps {
		if err := st.do(); err != nil {
			t.Fatalf("passo até %s: %v", st.reach, err)
		}
		if st.reach == s {
			return e
		}
	}
	t.Fatalf("status %s não alcançado", s)
	return nil
}

func TestNewExecution(t *testing.T) {
	e := newExec(t)
	if e.Status != StatusQueuedDiagnosis || !e.IsNew() || e.Version != 0 {
		t.Fatalf("estado inicial errado: %+v", e)
	}
	if !e.EnqueuedAt.Equal(t0) || !e.CreatedAt.Equal(t0) || e.Priority() != PriorityDiagnosis {
		t.Fatalf("datas/prioridade erradas: %+v", e)
	}
	ev := e.PendingEvents()
	if len(ev) != 1 || ev[0].Type != EventDiagnosisQueued || ev[0].From != "" || ev[0].To != StatusQueuedDiagnosis || ev[0].Actor != saga {
		t.Fatalf("evento de criação errado: %+v", ev)
	}
	e.ClearPendingEvents()
	if len(e.PendingEvents()) != 0 {
		t.Fatal("ClearPendingEvents não limpou")
	}
}

func TestNewExecution_SemOSID(t *testing.T) {
	if _, err := NewExecution(NewExecutionParams{OSID: "  "}, saga, t0); !errors.Is(err, domainerrors.ErrInvalidExecution) {
		t.Fatalf("esperava ErrInvalidExecution, veio %v", err)
	}
	if _, err := NewCancelledExecution("", "s", "x", saga, t0); !errors.Is(err, domainerrors.ErrInvalidExecution) {
		t.Fatalf("esperava ErrInvalidExecution, veio %v", err)
	}
}

func TestNewCancelledExecution(t *testing.T) {
	e, err := NewCancelledExecution("os-9", "saga-9", "cancelada antes de abrir", saga, t0)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != StatusCancelled || e.Reason != "cancelada antes de abrir" || e.CancelledAt == nil {
		t.Fatalf("execução cancelada errada: %+v", e)
	}
	ev := e.PendingEvents()
	if len(ev) != 1 || ev[0].Type != EventCancelled || ev[0].Details["reason"] != "cancelada antes de abrir" {
		t.Fatalf("evento errado: %+v", ev)
	}
}

func TestCaminhoFeliz(t *testing.T) {
	e := atStatus(t, StatusFinished)
	if e.Status != StatusFinished || e.Diagnosis == nil || len(e.Items) != 1 {
		t.Fatalf("estado final errado: %+v", e)
	}
	if e.DiagnosisStartedAt == nil || e.DiagnosedAt == nil || e.RepairStartedAt == nil || e.FinishedAt == nil {
		t.Fatalf("marcos de tempo não preenchidos: %+v", e)
	}
	if !e.Diagnosis.CompletedAt.Equal(t0.Add(2 * time.Minute)) {
		t.Fatalf("diagnóstico sem data de conclusão: %v", e.Diagnosis.CompletedAt)
	}
	if !e.EnqueuedAt.Equal(t0.Add(3 * time.Minute)) {
		t.Fatalf("EnqueuedAt deve ser a entrada na fila de reparo, veio %v", e.EnqueuedAt)
	}
	want := []EventType{EventDiagnosisQueued, EventDiagnosisStarted, EventDiagnosisCompleted, EventRepairQueued, EventRepairStarted, EventRepairFinished}
	ev := e.PendingEvents()
	if len(ev) != len(want) {
		t.Fatalf("esperava %d eventos, veio %d", len(want), len(ev))
	}
	for i, w := range want {
		if ev[i].Type != w || ev[i].ID == "" {
			t.Fatalf("evento %d = %+v, esperava %s", i, ev[i], w)
		}
	}
	if ev[2].Details["services"] != "1" || ev[2].Details["parts"] != "1" || ev[3].Details["items"] != "1" {
		t.Fatalf("detalhes errados: %+v %+v", ev[2].Details, ev[3].Details)
	}
}

func TestRejectRepair(t *testing.T) {
	e := atStatus(t, StatusRejected)
	if e.Status != StatusRejected || e.Reason != "sem vaga" {
		t.Fatalf("rejeição errada: %+v", e)
	}
	last := e.PendingEvents()[len(e.PendingEvents())-1]
	if last.Type != EventRepairRejected || last.From != StatusDiagnosed || last.Details["reason"] != "sem vaga" {
		t.Fatalf("evento de rejeição errado: %+v", last)
	}
}

func TestEnqueueRepair_CopiaItens(t *testing.T) {
	e := atStatus(t, StatusDiagnosed)
	items := []RepairItem{{Kind: "part", Ref: "X", Quantity: 1}}
	if err := e.EnqueueRepair(items, saga, t0); err != nil {
		t.Fatal(err)
	}
	items[0].Ref = "alterado"
	if e.Items[0].Ref != "X" {
		t.Fatal("a execução não pode compartilhar o slice do chamador")
	}
}

// Matriz de transições: cada operação só é aceita a partir dos status listados.
func TestTransicoes(t *testing.T) {
	ops := map[string]struct {
		from []Status
		do   func(e *Execution) error
	}{
		"BeginDiagnosis":    {[]Status{StatusQueuedDiagnosis}, func(e *Execution) error { return e.BeginDiagnosis(mechanic, t0) }},
		"CompleteDiagnosis": {[]Status{StatusInDiagnosis}, func(e *Execution) error { return e.CompleteDiagnosis(diag, mechanic, t0) }},
		"EnqueueRepair":     {[]Status{StatusDiagnosed}, func(e *Execution) error { return e.EnqueueRepair(nil, saga, t0) }},
		"RejectRepair":      {[]Status{StatusDiagnosed}, func(e *Execution) error { return e.RejectRepair("x", saga, t0) }},
		"BeginRepair":       {[]Status{StatusQueuedRepair}, func(e *Execution) error { return e.BeginRepair(mechanic, t0) }},
		"FinishRepair":      {[]Status{StatusInRepair}, func(e *Execution) error { return e.FinishRepair(mechanic, t0) }},
		"Cancel":            {ActiveStatuses, func(e *Execution) error { return e.Cancel("x", saga, t0) }},
	}
	all := []Status{StatusQueuedDiagnosis, StatusInDiagnosis, StatusDiagnosed, StatusQueuedRepair, StatusInRepair, StatusFinished, StatusRejected, StatusCancelled}
	for name, op := range ops {
		for _, s := range all {
			allowed := false
			for _, f := range op.from {
				allowed = allowed || f == s
			}
			t.Run(name+"/"+string(s), func(t *testing.T) {
				e := atStatus(t, s)
				before := len(e.PendingEvents())
				err := op.do(e)
				if allowed && err != nil {
					t.Fatalf("esperava sucesso, veio %v", err)
				}
				if !allowed {
					if !errors.Is(err, domainerrors.ErrInvalidTransition) {
						t.Fatalf("esperava ErrInvalidTransition, veio %v", err)
					}
					if e.Status != s || len(e.PendingEvents()) != before {
						t.Fatal("transição recusada não pode alterar o estado nem registrar evento")
					}
				}
			})
		}
	}
}

func TestCompleteDiagnosis_Invalido(t *testing.T) {
	e := atStatus(t, StatusInDiagnosis)
	err := e.CompleteDiagnosis(Diagnosis{}, mechanic, t0)
	if !errors.Is(err, domainerrors.ErrInvalidDiagnosis) || e.Status != StatusInDiagnosis {
		t.Fatalf("esperava ErrInvalidDiagnosis sem mudar status, veio %v (%s)", err, e.Status)
	}
}

func TestDiagnosisValidate(t *testing.T) {
	cases := map[string]struct {
		d     Diagnosis
		valid bool
	}{
		"vazio":               {Diagnosis{}, false},
		"só serviço":          {Diagnosis{Services: []DiagnosedService{{Code: 1}}}, true},
		"só peça":             {Diagnosis{Parts: []DiagnosedPart{{ManufacturerCode: "A", Quantity: 1}}}, true},
		"código zero":         {Diagnosis{Services: []DiagnosedService{{Code: 0}}}, false},
		"peça sem código":     {Diagnosis{Parts: []DiagnosedPart{{ManufacturerCode: " ", Quantity: 1}}}, false},
		"quantidade inválida": {Diagnosis{Parts: []DiagnosedPart{{ManufacturerCode: "A", Quantity: 0}}}, false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := c.d.Validate()
			if c.valid != (err == nil) {
				t.Fatalf("valid=%v, err=%v", c.valid, err)
			}
			if err != nil && !errors.Is(err, domainerrors.ErrInvalidDiagnosis) {
				t.Fatalf("erro deve embrulhar ErrInvalidDiagnosis: %v", err)
			}
		})
	}
}

func TestStatus(t *testing.T) {
	for _, s := range ActiveStatuses {
		if !s.IsActive() || s.IsFinal() {
			t.Errorf("%s deveria ser ativo", s)
		}
	}
	for _, s := range []Status{StatusFinished, StatusRejected, StatusCancelled} {
		if s.IsActive() || !s.IsFinal() {
			t.Errorf("%s deveria ser final", s)
		}
	}
	if Status("x").Valid() || Status("x").IsActive() {
		t.Error("status desconhecido não é válido")
	}
	if StatusQueuedRepair.Priority() != PriorityRepair || StatusInRepair.Priority() != PriorityRepair ||
		StatusQueuedDiagnosis.Priority() != PriorityDiagnosis || PriorityRepair >= PriorityDiagnosis {
		t.Error("reparo deve ter prioridade sobre diagnóstico")
	}
	if s, err := ParseStatus("in_repair"); err != nil || s != StatusInRepair {
		t.Errorf("ParseStatus: %v %v", s, err)
	}
	if _, err := ParseStatus("bogus"); !errors.Is(err, domainerrors.ErrInvalidStatus) {
		t.Errorf("esperava ErrInvalidStatus, veio %v", err)
	}
}

func TestServiceActor(t *testing.T) {
	if a := ServiceActor(""); a.ID != "unknown" || a.Type != ActorService {
		t.Fatalf("ator padrão errado: %+v", a)
	}
}
