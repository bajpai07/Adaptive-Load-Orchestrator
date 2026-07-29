package trip

import (
	"time"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type RiderStatus string

const (
	RiderStatusPicking   RiderStatus = "PICKING"
	RiderStatusEnRoute   RiderStatus = "EN_ROUTE"
	RiderStatusArrived   RiderStatus = "ARRIVED"
	RiderStatusCompleted RiderStatus = "COMPLETED"
)

type Rider struct {
	ID                    string      `json:"id"`
	Name                  string      `json:"name"` // e.g. "Rahul Sharma"
	CurrentLat            float64     `json:"current_lat"`
	CurrentLng            float64     `json:"current_lng"`
	PickupLat             float64     `json:"pickup_lat"`
	PickupLng             float64     `json:"pickup_lng"`
	DestinationGeofenceID string      `json:"destination_geofence_id"`
	DestinationLat        float64     `json:"destination_lat"`
	DestinationLng        float64     `json:"destination_lng"`
	AssignedOrderIDs      []string    `json:"assigned_order_ids"`
	Status                RiderStatus `json:"status"`
	SpeedKmH              float64     `json:"speed_kmh"`
	UpdatedAt             time.Time   `json:"updated_at"`
}

type TripStatus string

const (
	TripStatusAvailable  TripStatus = "AVAILABLE"
	TripStatusPooled     TripStatus = "POOLED"
	TripStatusDispatched TripStatus = "DISPATCHED"
	TripStatusCompleted  TripStatus = "COMPLETED"
)

type TripMember struct {
	OrderID         string `json:"order_id"`
	MemberID        string `json:"member_id"`
	DisplayName     string `json:"display_name"`      // e.g. "Aarav Mehta"
	FlatLocation    string `json:"flat_location"`     // e.g. "Flat 402, Tower B"
	ItemsSummary    string `json:"items_summary"`     // e.g. "Amul Taaza Milk (1L), Bread"
	OrderTotalPaise int64  `json:"order_total_paise"`  // e.g. 18500 (₹185.00)
	AvatarColor     string `json:"avatar_color"`      // e.g. "#8B5CF6"
}

type Trip struct {
	ID                     string       `json:"id"`
	RiderID                string       `json:"rider_id"`
	RiderName              string       `json:"rider_name"`               // e.g. "Rahul Sharma"
	GeofenceID             string       `json:"geofence_id"`
	GeofenceName           string       `json:"geofence_name"`             // e.g. "Aravali Heights, Tower B"
	AssignedOrderCount     int          `json:"assigned_order_count"`      // e.g. 4
	MemberOrderIDs         []string     `json:"member_order_ids"`
	Members                []TripMember `json:"members"`
	BaseDeliveryFeePaise   int64        `json:"base_delivery_fee_paise"`   // 3500 Paise = ₹35.00
	CurrentDeliveryFeePaise int64        `json:"current_delivery_fee_paise"`
	DiscountPaise          int64        `json:"discount_paise"`
	ETASeconds             float64      `json:"eta_seconds"`
	Status                 TripStatus   `json:"status"`
	CreatedAt              time.Time    `json:"created_at"`
	UpdatedAt              time.Time    `json:"updated_at"`
}

type TripEventType string

const (
	EventTripAvailable       TripEventType = "TRIP_AVAILABLE"
	EventTripUpdated         TripEventType = "TRIP_UPDATED"
	EventTripDiscountChanged TripEventType = "TRIP_DISCOUNT_CHANGED"
	EventTripCompleted       TripEventType = "TRIP_COMPLETED"
)

type TripEvent struct {
	Type                   TripEventType `json:"type"`
	TripID                 string        `json:"trip_id"`
	RiderID                string        `json:"rider_id"`
	RiderName              string        `json:"rider_name"`
	GeofenceID             string        `json:"geofence_id"`
	GeofenceName           string        `json:"geofence_name"`
	MemberCount            int           `json:"member_count"`
	AssignedOrderCount     int           `json:"assigned_order_count"`
	BaseDeliveryFeePaise   int64         `json:"base_delivery_fee_paise"`
	CurrentDeliveryFeePaise int64         `json:"current_delivery_fee_paise"`
	DiscountPaise          int64         `json:"discount_paise"`
	ETASeconds             float64       `json:"eta_seconds"`
	Timestamp              time.Time     `json:"timestamp"`
}

// CalculateDeliveryFee computes the pooled delivery fee per member based on total pooled order count.
func CalculateDeliveryFee(baseFeePaise int64, memberCount int) (currentFeePaise int64, discountPaise int64) {
	if memberCount <= 1 {
		return baseFeePaise, 0
	} else if memberCount == 2 {
		currentFeePaise = baseFeePaise / 2
		discountPaise = baseFeePaise - currentFeePaise
		return currentFeePaise, discountPaise
	} else {
		return 0, baseFeePaise
	}
}
