package fulfillment

import (
	"time"
)

type OrderStatus string

const (
	StatusQueued    OrderStatus = "QUEUED"
	StatusPicking   OrderStatus = "PICKING"
	StatusCompleted OrderStatus = "COMPLETED"
	StatusReRouted  OrderStatus = "RE_ROUTED"
)

type Order struct {
	ID                string        `json:"id"`
	StoreID           string        `json:"store_id"`
	OriginalStoreID   string        `json:"original_store_id"`
	MemberID          string        `json:"member_id,omitempty"`
	GroupCartID       string        `json:"group_cart_id,omitempty"`
	GeofenceID        string        `json:"geofence_id,omitempty"`
	IsPerishable      bool          `json:"is_perishable"`
	IsConsolidated    bool          `json:"is_consolidated"`
	ConsolidatedCount int           `json:"consolidated_count"`
	Status            OrderStatus   `json:"status"`
	QueuedAt          time.Time     `json:"queued_at"`
	PickStartedAt     time.Time     `json:"pick_started_at"`
	CompletedAt       time.Time     `json:"completed_at"`
	ServiceDuration   time.Duration `json:"service_duration"`
	TotalSystemTime   time.Duration `json:"total_system_time"`
}
