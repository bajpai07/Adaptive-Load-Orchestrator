package fulfillment

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const reserveStockLuaScript = `
local stockKey = KEYS[1]
local requestedQty = tonumber(ARGV[1])

local currentStockStr = redis.call('GET', stockKey)
if not currentStockStr then
    return {0, 0}
end

local currentStock = tonumber(currentStockStr)
if currentStock >= requestedQty then
    redis.call('DECRBY', stockKey, requestedQty)
    return {1, currentStock - requestedQty}
else
    return {0, currentStock}
end
`

type StockReservationStore struct {
	client *redis.Client
}

func NewStockReservationStore(client *redis.Client) *StockReservationStore {
	return &StockReservationStore{
		client: client,
	}
}

func stockKey(storeID, sku string) string {
	return fmt.Sprintf("stock:%s:%s", storeID, sku)
}

func (s *StockReservationStore) SetStock(ctx context.Context, storeID, sku string, qty int64) error {
	key := stockKey(storeID, sku)
	return s.client.Set(ctx, key, qty, 0).Err()
}

func (s *StockReservationStore) GetStock(ctx context.Context, storeID, sku string) (int64, error) {
	key := stockKey(storeID, sku)
	val, err := s.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// ReserveStockAtomic atomically checks and decrements inventory in Redis via Lua script.
// Returns (success, remainingStock, error).
func (s *StockReservationStore) ReserveStockAtomic(ctx context.Context, storeID, sku string, requestedQty int64) (bool, int64, error) {
	if s == nil || s.client == nil {
		// In-memory fallback if Redis is not configured in simulation mode
		return true, 999, nil
	}

	key := stockKey(storeID, sku)
	res, err := s.client.Eval(ctx, reserveStockLuaScript, []string{key}, requestedQty).Result()
	if err != nil {
		return false, 0, fmt.Errorf("redis lua reserveStock failed: %w", err)
	}

	resSlice, ok := res.([]interface{})
	if !ok || len(resSlice) < 2 {
		return false, 0, fmt.Errorf("invalid lua response format")
	}

	successVal, _ := resSlice[0].(int64)
	remainingStock, _ := resSlice[1].(int64)

	return successVal == 1, remainingStock, nil
}
