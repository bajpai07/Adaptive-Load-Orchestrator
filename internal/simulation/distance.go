package simulation

import (
	"math"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

const earthRadiusKm = 6371.0

// HaversineDistance computes the great-circle distance between two lat/lng pairs in kilometers.
func HaversineDistance(lat1, lng1, lat2, lng2 float64) float64 {
	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLng := (lng2 - lng1) * math.Pi / 180.0

	radLat1 := lat1 * math.Pi / 180.0
	radLat2 := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLng/2)*math.Sin(dLng/2)*math.Cos(radLat1)*math.Cos(radLat2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusKm * c
}
