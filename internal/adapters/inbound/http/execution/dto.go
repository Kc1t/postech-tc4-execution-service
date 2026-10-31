package executionhandler

import (
	"time"

	"github.com/Kc1t/postech-tc4-execution-service/internal/domain/entities"
	"github.com/Kc1t/postech-tc4-execution-service/internal/ports"
)

// ---- Requisições ----

// DiagnosedServiceRequest é um serviço do catálogo do os-service.
type DiagnosedServiceRequest struct {
	Code int `json:"code" binding:"required,gt=0" example:"10"`
}

// DiagnosedPartRequest é uma peça pelo código do fabricante.
type DiagnosedPartRequest struct {
	ManufacturerCode string `json:"manufacturer_code" binding:"required,max=64" example:"BOSCH-0986BB0"`
	Quantity         int    `json:"quantity" binding:"required,gt=0,lte=1000" example:"2"`
}

// CompleteDiagnosisRequest é o resultado do diagnóstico.
type CompleteDiagnosisRequest struct {
	Services []DiagnosedServiceRequest `json:"services" binding:"max=50,dive"`
	Parts    []DiagnosedPartRequest    `json:"parts" binding:"max=100,dive"`
	Notes    string                    `json:"notes" binding:"max=2000" example:"pastilhas dianteiras no limite"`
}

func (r CompleteDiagnosisRequest) toDomain() entities.Diagnosis {
	d := entities.Diagnosis{Notes: r.Notes}
	for _, s := range r.Services {
		d.Services = append(d.Services, entities.DiagnosedService{Code: s.Code})
	}
	for _, p := range r.Parts {
		d.Parts = append(d.Parts, entities.DiagnosedPart{ManufacturerCode: p.ManufacturerCode, Quantity: p.Quantity})
	}
	return d
}

// ---- Respostas ----

type VehicleResponse struct {
	Plate string `json:"plate" example:"ABC1D23"`
	Brand string `json:"brand,omitempty" example:"Fiat"`
	Model string `json:"model,omitempty" example:"Uno"`
	Year  int    `json:"year,omitempty" example:"2020"`
}

type DiagnosedServiceResponse struct {
	Code int `json:"code" example:"10"`
}

type DiagnosedPartResponse struct {
	ManufacturerCode string `json:"manufacturer_code" example:"BOSCH-0986BB0"`
	Quantity         int    `json:"quantity" example:"2"`
}

type DiagnosisResponse struct {
	Services    []DiagnosedServiceResponse `json:"services"`
	Parts       []DiagnosedPartResponse    `json:"parts"`
	Notes       string                     `json:"notes,omitempty"`
	CompletedAt time.Time                  `json:"completed_at"`
}

type RepairItemResponse struct {
	Kind        string  `json:"kind" example:"part"`
	Ref         string  `json:"ref" example:"BOSCH-0986BB0"`
	Description string  `json:"description,omitempty" example:"Pastilha de freio"`
	Quantity    int     `json:"quantity" example:"2"`
	UnitPrice   float64 `json:"unit_price" example:"89.9"`
}

// ExecutionResponse é o estado atual da execução.
type ExecutionResponse struct {
	OSID               string               `json:"os_id" example:"6f1c1f7e-8d0a-4c49-9d64-3b8f0a2b7c11"`
	OSCode             int                  `json:"os_code" example:"42"`
	SagaID             string               `json:"saga_id" example:"0b8e6c3a-2f4d-4e21-a1b7-9c0d5e6f7a81"`
	Status             string               `json:"status" example:"queued_diagnosis"`
	Priority           int                  `json:"priority" example:"2"`
	Vehicle            VehicleResponse      `json:"vehicle"`
	Notes              string               `json:"notes,omitempty" example:"barulho no freio"`
	Diagnosis          *DiagnosisResponse   `json:"diagnosis,omitempty"`
	Items              []RepairItemResponse `json:"items,omitempty"`
	Reason             string               `json:"reason,omitempty"`
	EnqueuedAt         time.Time            `json:"enqueued_at"`
	CreatedAt          time.Time            `json:"created_at"`
	UpdatedAt          time.Time            `json:"updated_at"`
	DiagnosisStartedAt *time.Time           `json:"diagnosis_started_at,omitempty"`
	DiagnosedAt        *time.Time           `json:"diagnosed_at,omitempty"`
	RepairStartedAt    *time.Time           `json:"repair_started_at,omitempty"`
	FinishedAt         *time.Time           `json:"finished_at,omitempty"`
	CancelledAt        *time.Time           `json:"cancelled_at,omitempty"`
	Version            int                  `json:"version" example:"1"`
}

// TimelineEventResponse é um registro da linha do tempo.
type TimelineEventResponse struct {
	ID         string            `json:"id"`
	Type       string            `json:"type" example:"diagnosis_started"`
	FromStatus string            `json:"from_status,omitempty" example:"queued_diagnosis"`
	ToStatus   string            `json:"to_status" example:"in_diagnosis"`
	ActorID    string            `json:"actor_id" example:"mec-1"`
	ActorType  string            `json:"actor_type" example:"mechanic"`
	Details    map[string]string `json:"details,omitempty"`
	OccurredAt time.Time         `json:"occurred_at"`
}

// ExecutionDetailsResponse é a execução com a linha do tempo.
type ExecutionDetailsResponse struct {
	ExecutionResponse
	Timeline []TimelineEventResponse `json:"timeline"`
}

// QueueResponse é a fila de trabalho.
type QueueResponse struct {
	Items []ExecutionResponse `json:"items"`
	Count int                 `json:"count" example:"1"`
}

func toResponse(e *entities.Execution) ExecutionResponse {
	r := ExecutionResponse{
		OSID:               e.OSID,
		OSCode:             e.OSCode,
		SagaID:             e.SagaID,
		Status:             string(e.Status),
		Priority:           e.Priority(),
		Vehicle:            VehicleResponse(e.Vehicle),
		Notes:              e.Notes,
		Reason:             e.Reason,
		EnqueuedAt:         e.EnqueuedAt,
		CreatedAt:          e.CreatedAt,
		UpdatedAt:          e.UpdatedAt,
		DiagnosisStartedAt: e.DiagnosisStartedAt,
		DiagnosedAt:        e.DiagnosedAt,
		RepairStartedAt:    e.RepairStartedAt,
		FinishedAt:         e.FinishedAt,
		CancelledAt:        e.CancelledAt,
		Version:            e.Version,
	}
	if d := e.Diagnosis; d != nil {
		dr := &DiagnosisResponse{
			Services: make([]DiagnosedServiceResponse, 0, len(d.Services)),
			Parts:    make([]DiagnosedPartResponse, 0, len(d.Parts)),
			Notes:    d.Notes, CompletedAt: d.CompletedAt,
		}
		for _, s := range d.Services {
			dr.Services = append(dr.Services, DiagnosedServiceResponse(s))
		}
		for _, p := range d.Parts {
			dr.Parts = append(dr.Parts, DiagnosedPartResponse(p))
		}
		r.Diagnosis = dr
	}
	for _, i := range e.Items {
		r.Items = append(r.Items, RepairItemResponse(i))
	}
	return r
}

func toDetailsResponse(d *ports.ExecutionDetails) ExecutionDetailsResponse {
	out := ExecutionDetailsResponse{ExecutionResponse: toResponse(d.Execution), Timeline: make([]TimelineEventResponse, 0, len(d.Timeline))}
	for _, ev := range d.Timeline {
		out.Timeline = append(out.Timeline, TimelineEventResponse{
			ID:         ev.ID,
			Type:       string(ev.Type),
			FromStatus: string(ev.From),
			ToStatus:   string(ev.To),
			ActorID:    ev.Actor.ID,
			ActorType:  ev.Actor.Type,
			Details:    ev.Details,
			OccurredAt: ev.OccurredAt,
		})
	}
	return out
}
