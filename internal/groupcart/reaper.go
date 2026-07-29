package groupcart

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// TTLReaper periodically checks active group carts in Redis and finalizes expired carts.
type TTLReaper struct {
	store    *RedisCartStore
	client   *redis.Client
	interval time.Duration
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func NewTTLReaper(store *RedisCartStore, client *redis.Client, checkInterval time.Duration) *TTLReaper {
	if checkInterval <= 0 {
		checkInterval = 1 * time.Second
	}
	return &TTLReaper{
		store:    store,
		client:   client,
		interval: checkInterval,
	}
}

func (r *TTLReaper) Start(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)
	r.wg.Add(1)
	go r.loop(ctx)
}

func (r *TTLReaper) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

func (r *TTLReaper) loop(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reapExpiredCarts(ctx)
		}
	}
}

func (r *TTLReaper) reapExpiredCarts(ctx context.Context) {
	keys, err := r.client.Keys(ctx, "cart:*").Result()
	if err != nil || len(keys) == 0 {
		return
	}

	now := time.Now()
	for _, key := range keys {
		cartID := key[5:] // Trim "cart:" prefix
		cart, err := r.store.GetCart(ctx, cartID)
		if err != nil || cart.Status != CartStatusActive {
			continue
		}

		if now.After(cart.ExpiresAt) {
			log.Printf("[TTL REAPER] Cart %s expired at %v (Now: %v). Finalizing cart...", cart.ID, cart.ExpiresAt, now)
			_, err := r.store.FinalizeCart(ctx, cart.ID)
			if err != nil {
				log.Printf("[TTL REAPER ERROR] Failed to finalize expired cart %s: %v", cart.ID, err)
			}
		}
	}
}
