package groupcart

import (
	"time"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type Member struct {
	ID          string    `json:"id"`
	DisplayName string    `json:"display_name"`
	JoinedAt    time.Time `json:"joined_at"`
}

type CartItem struct {
	ID              string    `json:"id"`
	SKU             string    `json:"sku"`
	Name            string    `json:"name"`
	PricePaise      int64     `json:"price_paise"` // Integer Paise (100 Paise = ₹1.00)
	AddedByMemberID string    `json:"added_by_member_id"`
	AddedAt         time.Time `json:"added_at"`
}

type CartStatus string

const (
	CartStatusActive    CartStatus = "ACTIVE"
	CartStatusFinalized CartStatus = "FINALIZED"
	CartStatusExpired   CartStatus = "EXPIRED"
)

type GroupCart struct {
	ID                   string     `json:"id"`
	GeofenceID           string     `json:"geofence_id"`
	GeofenceCentroid     Location   `json:"geofence_centroid"`
	GeofenceRadiusMeters float64    `json:"geofence_radius_meters"`
	Members              []Member   `json:"members"`
	Items                []CartItem `json:"items"`
	CreatedAt            time.Time  `json:"created_at"`
	ExpiresAt            time.Time  `json:"expires_at"`
	UnlockThresholdPaise int64      `json:"unlock_threshold_paise"`
	Unlocked             bool       `json:"unlocked"`
	TotalPaise           int64      `json:"total_paise"`
	Status               CartStatus `json:"status"`
}

type EventType string

const (
	EventCartUpdated           EventType = "CART_UPDATED"
	EventCartUnlocked          EventType = "CART_UNLOCKED"
	EventCartFinalized         EventType = "CART_FINALIZED"
	EventMemberJoined          EventType = "MEMBER_JOINED"
	EventSubstitutionSuggested EventType = "SUBSTITUTION_SUGGESTED"
)

type SubstitutePayload struct {
	SKU               string  `json:"sku"`
	Name              string  `json:"name"`
	PricePaise        int64   `json:"price_paise"`
	Category          string  `json:"category"`
	Brand             string  `json:"brand"`
	TotalScore        float64 `json:"total_score"`
	ExplanationReason string  `json:"explanation_reason"`
}

type CartEvent struct {
	Type                 EventType          `json:"type"`
	CartID               string             `json:"cart_id"`
	MemberID             string             `json:"member_id,omitempty"`
	Item                 *CartItem          `json:"item,omitempty"`
	Cart                 *GroupCart         `json:"cart,omitempty"`
	SuggestedSubstitute  *SubstitutePayload `json:"suggested_substitute,omitempty"`
	TotalPaise           int64              `json:"total_paise"`
	UnlockThresholdPaise int64              `json:"unlock_threshold_paise"`
	Unlocked             bool               `json:"unlocked"`
	Status               CartStatus         `json:"status,omitempty"`
	Timestamp            time.Time          `json:"timestamp"`
}
