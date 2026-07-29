package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"time"

	"adaptive-load-orchestrator/internal/groupcart"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func main() {
	clientsFlag := flag.Int("clients", 250, "Number of concurrent WebSocket clients to simulate")
	geofenceFlag := flag.String("geofence", "geofence-aravali", "Geofence ID for group cart")

	flag.Parse()

	targetClients := *clientsFlag
	geofenceID := *geofenceFlag

	log.Printf("=========================================================")
	log.Printf("   ADAPTIVE LOAD ORCHESTRATOR — PHASE 2 BENCHMARK")
	log.Printf("=========================================================")
	log.Printf("Target WS Clients  : %d", targetClients)
	log.Printf("Geofence Target    : %s", geofenceID)
	log.Printf("---------------------------------------------------------")

	// Configure Redis Connection (REDIS_URL or REDIS_ADDR env vars, fallback to embedded miniredis)
	var rdb *redis.Client
	var mr *miniredis.Miniredis

	redisURL := os.Getenv("REDIS_URL")
	redisAddr := os.Getenv("REDIS_ADDR")

	if redisURL != "" {
		opt, err := redis.ParseURL(redisURL)
		if err != nil {
			log.Fatalf("[FATAL] Invalid REDIS_URL=%s: %v", redisURL, err)
		}
		rdb = redis.NewClient(opt)
		log.Printf("Redis Connection    : External REDIS_URL (%s)", opt.Addr)
	} else if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
		log.Printf("Redis Connection    : External REDIS_ADDR (%s)", redisAddr)
	} else {
		var err error
		mr, err = miniredis.Run()
		if err != nil {
			log.Fatalf("[FATAL] Failed to start miniredis: %v", err)
		}
		defer mr.Close()
		rdb = redis.NewClient(&redis.Options{Addr: mr.Addr()})
		log.Printf("Redis Connection    : Embedded Miniredis (%s)", mr.Addr())
	}
	defer rdb.Close()

	store := groupcart.NewRedisCartStore(rdb)
	server := groupcart.NewServer(store)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/carts/join", server.HandleJoinCart)
	mux.HandleFunc("/api/carts/item", server.HandleAddItem)
	mux.HandleFunc("/ws/cart", server.HandleWebSocket)

	httptestServer := httptest.NewServer(mux)
	defer httptestServer.Close()

	ctx := context.Background()

	// Initial member creates/joins cart
	creator := groupcart.Member{ID: "mem-creator", DisplayName: "Creator"}
	cart, err := store.CreateOrGetActiveCart(ctx, geofenceID, creator, groupcart.Location{Lat: 28.6315, Lng: 77.2167}, 500, 30*time.Minute, 20000)
	if err != nil {
		log.Fatalf("Failed to create cart: %v", err)
	}

	wsURL, _ := url.Parse(httptestServer.URL)
	wsURL.Scheme = "ws"
	wsURL.Path = "/ws/cart"

	clientCounts := []int{10, 50, 100, 250}
	if targetClients > 250 {
		clientCounts = append(clientCounts, targetClients)
	}

	fmt.Println("\n=========================================================================================================")
	fmt.Println("                             WEBSOCKET CONCURRENT LOAD BENCHMARK RESULTS                                 ")
	fmt.Println("=========================================================================================================")
	fmt.Printf("%-15s | %-20s | %-22s | %-22s\n", "Concurrent WS", "Avg Setup Time (ms)", "Broadcast Latency (ms)", "Status")
	fmt.Println("---------------------------------------------------------------------------------------------------------")

	for _, count := range clientCounts {
		if count > targetClients {
			continue
		}

		conns := make([]*websocket.Conn, count)
		var setupTimeSum time.Duration
		var setupMu sync.Mutex

		var wg sync.WaitGroup

		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				t0 := time.Now()
				u := fmt.Sprintf("%s?cart_id=%s&member_id=mem-sim-%d", wsURL.String(), cart.ID, idx+1)
				conn, _, err := websocket.DefaultDialer.Dial(u, nil)
				dt := time.Since(t0)

				if err == nil {
					setupMu.Lock()
					conns[idx] = conn
					setupTimeSum += dt
					setupMu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		activeConns := 0
		for _, c := range conns {
			if c != nil {
				activeConns++
			}
		}

		avgSetupMs := float64(setupTimeSum.Milliseconds()) / float64(activeConns)

		// Broadcast latency measurement
		time.Sleep(50 * time.Millisecond) // Allow WS registration loop to settle

		itemPayload, _ := json.Marshal(map[string]interface{}{
			"cart_id":            cart.ID,
			"sku":                "SKU-BENCH",
			"name":               "Bench Item",
			"price_paise":        500,
			"added_by_member_id": "mem-creator",
		})

		writeStart := time.Now()
		resp, err := http.Post(httptestServer.URL+"/api/carts/item", "application/json", bytes.NewBuffer(itemPayload))
		if err != nil {
			log.Printf("AddItem HTTP failed: %v", err)
		} else {
			resp.Body.Close()
		}

		// Collect WS broadcast receipts
		var broadcastDurations []time.Duration
		var broadcastMu sync.Mutex
		var bWg sync.WaitGroup

		for i := 0; i < activeConns; i++ {
			bWg.Add(1)
			go func(idx int) {
				defer bWg.Done()
				if conns[idx] == nil {
					return
				}
				conns[idx].SetReadDeadline(time.Now().Add(2 * time.Second))
				_, _, err := conns[idx].ReadMessage()
				dt := time.Since(writeStart)
				if err == nil {
					broadcastMu.Lock()
					broadcastDurations = append(broadcastDurations, dt)
					broadcastMu.Unlock()
				}
			}(i)
		}

		bWg.Wait()

		// Cleanup connections
		for _, c := range conns {
			if c != nil {
				c.Close()
			}
		}

		var avgBroadcastMs float64
		if len(broadcastDurations) > 0 {
			var totalDur time.Duration
			for _, d := range broadcastDurations {
				totalDur += d
			}
			avgBroadcastMs = float64(totalDur.Milliseconds()) / float64(len(broadcastDurations))
		}

		status := "PASS (Optimal)"
		if avgBroadcastMs > 200 {
			status = "DEGRADED (>200ms)"
		}

		fmt.Printf("%-15d | %-20.2f | %-22.2f | %-22s\n", activeConns, avgSetupMs, avgBroadcastMs, status)
	}

	fmt.Println("=========================================================================================================")
	fmt.Println()
}
