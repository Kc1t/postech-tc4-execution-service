package entities

import "time"

// EventType é o tipo de um registro da linha do tempo.
type EventType string

const (
	EventDiagnosisQueued    EventType = "diagnosis_queued"
	EventDiagnosisStarted   EventType = "diagnosis_started"
	EventDiagnosisCompleted EventType = "diagnosis_completed"
	EventRepairQueued       EventType = "repair_queued"
	EventRepairRejected     EventType = "repair_rejected"
	EventRepairStarted      EventType = "repair_started"
	EventRepairFinished     EventType = "repair_finished"
	EventCancelled          EventType = "execution_cancelled"
)

// Tipos de ator registrados na linha do tempo.
const (
	ActorService  = "service"  // mensagem de outro serviço (saga)
	ActorAdmin    = "admin"    // usuário administrador
	ActorMechanic = "mechanic" // mecânico
)

// Actor é quem provocou a mudança: um usuário (sub do JWT) ou um serviço (producer da mensagem).
type Actor struct {
	ID   string
	Type string
}

// ServiceActor representa um serviço que enviou um comando.
func ServiceActor(name string) Actor {
	if name == "" {
		name = "unknown"
	}
	return Actor{ID: name, Type: ActorService}
}

// TimelineEvent é um registro imutável de mudança de status.
type TimelineEvent struct {
	ID         string
	Type       EventType
	From       Status
	To         Status
	Actor      Actor
	Details    map[string]string
	OccurredAt time.Time
}
