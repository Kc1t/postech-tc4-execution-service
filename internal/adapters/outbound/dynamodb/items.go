package dynamodb

import (
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
)

// stateItem é o item STATE: o estado atual da execução.
type stateItem struct {
	PK     string `dynamodbav:"pk"`
	SK     string `dynamodbav:"sk"`
	GSI1PK string `dynamodbav:"gsi1pk,omitempty"`
	GSI1SK string `dynamodbav:"gsi1sk,omitempty"`
	Kind   string `dynamodbav:"kind"`

	OSID       string         `dynamodbav:"os_id"`
	OSCode     int            `dynamodbav:"os_code"`
	SagaID     string         `dynamodbav:"saga_id"`
	Vehicle    vehicleAttr    `dynamodbav:"vehicle"`
	Notes      string         `dynamodbav:"notes,omitempty"`
	Status     string         `dynamodbav:"status"`
	Priority   int            `dynamodbav:"priority"`
	Diagnosis  *diagnosisAttr `dynamodbav:"diagnosis,omitempty"`
	Items      []itemAttr     `dynamodbav:"items,omitempty"`
	Reason     string         `dynamodbav:"reason,omitempty"`
	EnqueuedAt time.Time      `dynamodbav:"enqueued_at"`

	CreatedAt          time.Time  `dynamodbav:"created_at"`
	UpdatedAt          time.Time  `dynamodbav:"updated_at"`
	DiagnosisStartedAt *time.Time `dynamodbav:"diagnosis_started_at,omitempty"`
	DiagnosedAt        *time.Time `dynamodbav:"diagnosed_at,omitempty"`
	RepairStartedAt    *time.Time `dynamodbav:"repair_started_at,omitempty"`
	FinishedAt         *time.Time `dynamodbav:"finished_at,omitempty"`
	CancelledAt        *time.Time `dynamodbav:"cancelled_at,omitempty"`

	Version int `dynamodbav:"version"`
}

type vehicleAttr struct {
	Plate string `dynamodbav:"plate,omitempty"`
	Brand string `dynamodbav:"brand,omitempty"`
	Model string `dynamodbav:"model,omitempty"`
	Year  int    `dynamodbav:"year,omitempty"`
}

type diagnosisAttr struct {
	Services    []serviceAttr `dynamodbav:"services"`
	Parts       []partAttr    `dynamodbav:"parts"`
	Notes       string        `dynamodbav:"notes,omitempty"`
	CompletedAt time.Time     `dynamodbav:"completed_at"`
}

type serviceAttr struct {
	Code int `dynamodbav:"code"`
}

type partAttr struct {
	ManufacturerCode string `dynamodbav:"manufacturer_code"`
	Quantity         int    `dynamodbav:"quantity"`
}

type itemAttr struct {
	Kind        string  `dynamodbav:"kind"`
	Ref         string  `dynamodbav:"ref"`
	Description string  `dynamodbav:"description,omitempty"`
	Quantity    int     `dynamodbav:"quantity"`
	UnitPrice   float64 `dynamodbav:"unit_price"`
}

// eventItem é um registro da linha do tempo (EVT#).
type eventItem struct {
	PK         string            `dynamodbav:"pk"`
	SK         string            `dynamodbav:"sk"`
	Kind       string            `dynamodbav:"kind"`
	ID         string            `dynamodbav:"id"`
	OSID       string            `dynamodbav:"os_id"`
	Type       string            `dynamodbav:"type"`
	From       string            `dynamodbav:"from_status,omitempty"`
	To         string            `dynamodbav:"to_status"`
	ActorID    string            `dynamodbav:"actor_id"`
	ActorType  string            `dynamodbav:"actor_type"`
	Details    map[string]string `dynamodbav:"details,omitempty"`
	OccurredAt time.Time         `dynamodbav:"occurred_at"`
}

// outboxItem é uma mensagem do outbox.
type outboxItem struct {
	PK              string            `dynamodbav:"pk"`
	SK              string            `dynamodbav:"sk"`
	Kind            string            `dynamodbav:"kind"`
	MessageID       string            `dynamodbav:"message_id"`
	MessageType     string            `dynamodbav:"message_type"`
	SagaID          string            `dynamodbav:"saga_id,omitempty"`
	OSID            string            `dynamodbav:"os_id,omitempty"`
	DestinationKind string            `dynamodbav:"destination_kind"`
	Destination     string            `dynamodbav:"destination"`
	Body            string            `dynamodbav:"body"`
	TraceHeaders    map[string]string `dynamodbav:"trace_headers,omitempty"`
	Published       bool              `dynamodbav:"published"`
	CreatedAt       time.Time         `dynamodbav:"created_at"`
	PublishedAt     *time.Time        `dynamodbav:"published_at,omitempty"`
	Attempts        int               `dynamodbav:"attempts"`
	LastError       string            `dynamodbav:"last_error,omitempty"`
	LeaseUntil      int64             `dynamodbav:"lease_until,omitempty"` // epoch ms
	LeaseOwner      string            `dynamodbav:"lease_owner,omitempty"`
	TTL             int64             `dynamodbav:"ttl,omitempty"` // epoch s
}

// inboxItem registra uma mensagem consumida.
type inboxItem struct {
	PK          string    `dynamodbav:"pk"`
	SK          string    `dynamodbav:"sk"`
	Kind        string    `dynamodbav:"kind"`
	Consumer    string    `dynamodbav:"consumer"`
	MessageID   string    `dynamodbav:"message_id"`
	MessageType string    `dynamodbav:"message_type"`
	ProcessedAt time.Time `dynamodbav:"processed_at"`
	TTL         int64     `dynamodbav:"ttl"` // epoch s
}

func toStateItem(e *entities.Execution, version int) stateItem {
	it := stateItem{
		PK:                 execPK(e.OSID),
		SK:                 skState,
		Kind:               kindExecution,
		OSID:               e.OSID,
		OSCode:             e.OSCode,
		SagaID:             e.SagaID,
		Vehicle:            vehicleAttr(e.Vehicle),
		Notes:              e.Notes,
		Status:             string(e.Status),
		Priority:           e.Priority(),
		Reason:             e.Reason,
		EnqueuedAt:         e.EnqueuedAt,
		CreatedAt:          e.CreatedAt,
		UpdatedAt:          e.UpdatedAt,
		DiagnosisStartedAt: e.DiagnosisStartedAt,
		DiagnosedAt:        e.DiagnosedAt,
		RepairStartedAt:    e.RepairStartedAt,
		FinishedAt:         e.FinishedAt,
		CancelledAt:        e.CancelledAt,
		Version:            version,
	}
	if e.Status.IsActive() {
		it.GSI1PK = queuePK(e.Status)
		it.GSI1SK = queueSK(e.Priority(), e.EnqueuedAt)
	}
	if d := e.Diagnosis; d != nil {
		da := &diagnosisAttr{
			Services: make([]serviceAttr, 0, len(d.Services)), Parts: make([]partAttr, 0, len(d.Parts)),
			Notes: d.Notes, CompletedAt: d.CompletedAt,
		}
		for _, s := range d.Services {
			da.Services = append(da.Services, serviceAttr(s))
		}
		for _, p := range d.Parts {
			da.Parts = append(da.Parts, partAttr(p))
		}
		it.Diagnosis = da
	}
	for _, i := range e.Items {
		it.Items = append(it.Items, itemAttr(i))
	}
	return it
}

func (it stateItem) toEntity() *entities.Execution {
	e := &entities.Execution{
		OSID:               it.OSID,
		OSCode:             it.OSCode,
		SagaID:             it.SagaID,
		Vehicle:            entities.Vehicle(it.Vehicle),
		Notes:              it.Notes,
		Status:             entities.Status(it.Status),
		Reason:             it.Reason,
		EnqueuedAt:         it.EnqueuedAt,
		CreatedAt:          it.CreatedAt,
		UpdatedAt:          it.UpdatedAt,
		DiagnosisStartedAt: it.DiagnosisStartedAt,
		DiagnosedAt:        it.DiagnosedAt,
		RepairStartedAt:    it.RepairStartedAt,
		FinishedAt:         it.FinishedAt,
		CancelledAt:        it.CancelledAt,
		Version:            it.Version,
	}
	if da := it.Diagnosis; da != nil {
		d := &entities.Diagnosis{Notes: da.Notes, CompletedAt: da.CompletedAt}
		for _, s := range da.Services {
			d.Services = append(d.Services, entities.DiagnosedService(s))
		}
		for _, p := range da.Parts {
			d.Parts = append(d.Parts, entities.DiagnosedPart(p))
		}
		e.Diagnosis = d
	}
	for _, i := range it.Items {
		e.Items = append(e.Items, entities.RepairItem(i))
	}
	return e
}

func toEventItem(osID string, ev entities.TimelineEvent) eventItem {
	return eventItem{
		PK:         execPK(osID),
		SK:         eventSK(ev.OccurredAt, ev.ID),
		Kind:       kindTimeline,
		ID:         ev.ID,
		OSID:       osID,
		Type:       string(ev.Type),
		From:       string(ev.From),
		To:         string(ev.To),
		ActorID:    ev.Actor.ID,
		ActorType:  ev.Actor.Type,
		Details:    ev.Details,
		OccurredAt: ev.OccurredAt,
	}
}

func (it eventItem) toEntity() entities.TimelineEvent {
	return entities.TimelineEvent{
		ID:         it.ID,
		Type:       entities.EventType(it.Type),
		From:       entities.Status(it.From),
		To:         entities.Status(it.To),
		Actor:      entities.Actor{ID: it.ActorID, Type: it.ActorType},
		Details:    it.Details,
		OccurredAt: it.OccurredAt,
	}
}
