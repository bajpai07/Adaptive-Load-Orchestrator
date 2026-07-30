package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"adaptive-load-orchestrator/internal/fulfillment"
	"adaptive-load-orchestrator/internal/groupcart"
	"adaptive-load-orchestrator/internal/simulation"
	"adaptive-load-orchestrator/internal/trip"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

type StoreWrapper struct {
	store *fulfillment.Store
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type OpsServer struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]bool
}

func NewOpsServer() *OpsServer {
	return &OpsServer{
		clients: make(map[*websocket.Conn]bool),
	}
}

func (s *OpsServer) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()
		conn.Close()
	}()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}
}

func (s *OpsServer) BroadcastEvent(ev *fulfillment.UnifiedDecisionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(ev)
	if err != nil {
		return
	}

	for conn := range s.clients {
		_ = conn.WriteMessage(websocket.TextMessage, data)
	}
}

func (s *OpsServer) BroadcastTripEvent(ev *trip.TripEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(ev)
	if err != nil {
		return
	}

	for conn := range s.clients {
		_ = conn.WriteMessage(websocket.TextMessage, data)
	}
}

type ScheduledOrder struct {
	SimTimeOffset time.Duration
	Order         *fulfillment.Order
}

func main() {
	modeFlag := flag.String("mode", "full-orchestration", "Decision engine mode: naive | cost-gated | full-orchestration")
	durationFlag := flag.Duration("duration", 10*time.Minute, "Simulated duration (e.g. 10m, 30m)")
	storesCountFlag := flag.Int("stores", 8, "Number of dark stores in network")
	timeScaleFlag := flag.Float64("time-scale", 100.0, "Simulation speedup factor (e.g. 100.0 means 10m sim runs in 6s)")
	surgeStoreFlag := flag.String("surge-store", "", "Store ID to apply surge (e.g. store-1)")
	surgeFactorFlag := flag.Float64("surge-factor", 3.0, "Surge arrival rate multiplier")
	surgeDurationFlag := flag.Duration("surge-duration", 5*time.Minute, "Duration of the surge window")
	surgeStartFlag := flag.Duration("surge-start", 2*time.Minute, "Start offset of surge window")
	seedFlag := flag.Int64("seed", 42, "Fixed random seed for controlled reproducible demand generation")
	gridSpacingFlag := flag.Float64("grid-spacing", 0.010, "Lat/Lng grid spacing offset (0.010 ~= 1.11km, 0.004 ~= 0.44km)")
	portFlag := flag.Int("port", 8081, "Ops dashboard HTTP server port")
	continuousFlag := flag.Bool("continuous", false, "Run simulation in a continuous loop for live deployment demos")

	flag.Parse()

	engineMode := fulfillment.DecisionEngineMode(*modeFlag)
	if envMode := os.Getenv("MODE"); envMode != "" {
		engineMode = fulfillment.DecisionEngineMode(envMode)
	}

	simDuration := *durationFlag
	numStores := *storesCountFlag
	timeScale := *timeScaleFlag
	surgeStore := *surgeStoreFlag
	surgeFactor := *surgeFactorFlag
	surgeDuration := *surgeDurationFlag
	surgeStart := *surgeStartFlag
	fixedSeed := *seedFlag
	gridSpacing := *gridSpacingFlag

	// Determine auto-restart / continuous simulation mode
	autoRestart := *continuousFlag
	if envRestart := os.Getenv("AUTO_RESTART"); envRestart == "true" || envRestart == "1" {
		autoRestart = true
	}
	if envContinuous := os.Getenv("CONTINUOUS"); envContinuous == "true" || envContinuous == "1" {
		autoRestart = true
	}
	// Default to continuous mode when deployed in cloud env (PORT set and no explicit single-run requested)
	if os.Getenv("PORT") != "" && !*continuousFlag {
		if os.Getenv("AUTO_RESTART") != "false" && os.Getenv("CONTINUOUS") != "false" {
			autoRestart = true
		}
	}

	// Read PORT environment variable if set (overrides --port flag for Railway/Render/Cloud deployment)
	port := *portFlag
	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil && p > 0 {
			port = p
		}
	}

	// Validate timeScale parameter
	if timeScale <= 0.0 || timeScale > 1000.0 {
		log.Fatalf("[FATAL] Invalid --time-scale=%.1f (Must be between 0.1 and 1000.0)", timeScale)
	}

	realDuration := time.Duration(float64(simDuration) / timeScale)

	log.Printf("=========================================================")
	log.Printf("   ADAPTIVE LOAD ORCHESTRATOR — PHASE 5 SIMULATION")
	log.Printf("=========================================================")
	log.Printf("Engine Mode         : %s", engineMode)
	log.Printf("Simulated Duration  : %v (Real execution time: %v)", simDuration, realDuration)
	log.Printf("Dark Stores Count   : %d", numStores)
	log.Printf("Time Speedup Scale  : %.1fx", timeScale)
	log.Printf("Fixed Random Seed   : %d (Controlled Deterministic Demand)", fixedSeed)
	log.Printf("Grid Spacing Offset : %.4f", gridSpacing)
	log.Printf("HTTP Server Port    : %d", port)
	log.Printf("Auto Restart Mode   : %v", autoRestart)
	if surgeStore != "" {
		log.Printf("Surge Configured    : %s (%.1fx rate for %v starting at +%v)",
			surgeStore, surgeFactor, surgeDuration, surgeStart)
	} else {
		log.Printf("Surge Configured    : None (Baseline run)")
	}

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
	log.Printf("---------------------------------------------------------")

	cartStore := groupcart.NewRedisCartStore(rdb)
	resStore := fulfillment.NewStockReservationStore(rdb)
	tripStore := trip.NewRedisTripStore(rdb)

	// Start Ops WebSocket & Group Cart API Server (runs indefinitely)
	opsServer := NewOpsServer()
	groupCartServer := groupcart.NewServer(cartStore)

	// Subscribe to Redis trip_events and broadcast via WebSocket
	go func() {
		ch := tripStore.SubscribeTripEvents(context.Background())
		for ev := range ch {
			opsServer.BroadcastTripEvent(ev)
		}
	}()

	go func() {
		http.HandleFunc("/ws/ops", opsServer.HandleWS)
		http.HandleFunc("/api/carts/join", groupCartServer.HandleJoinCart)
		http.HandleFunc("/api/carts/item", groupCartServer.HandleAddItem)
		http.HandleFunc("/ws/cart", groupCartServer.HandleWebSocket)

		// Phase 5 Rider Trip Endpoints
		http.HandleFunc("/api/trips/join", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				TripID          string `json:"trip_id"`
				OrderID         string `json:"order_id"`
				MemberID        string `json:"member_id"`
				DisplayName     string `json:"display_name"`
				FlatLocation    string `json:"flat_location"`
				ItemsSummary    string `json:"items_summary"`
				OrderTotalPaise int64  `json:"order_total_paise"`
				AvatarColor     string `json:"avatar_color"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if req.TripID == "" {
				req.TripID = "trip-rider-1"
			}
			if req.OrderID == "" {
				req.OrderID = fmt.Sprintf("ord-web-%d", time.Now().UnixNano()%10000)
			}
			if req.DisplayName == "" {
				req.DisplayName = "You (Web User)"
			}
			if req.FlatLocation == "" {
				req.FlatLocation = "Flat 304, Tower B"
			}
			if req.ItemsSummary == "" {
				req.ItemsSummary = "Organic Eggs (6-pack), Greek Yogurt"
			}
			if req.OrderTotalPaise == 0 {
				req.OrderTotalPaise = 22000
			}
			if req.AvatarColor == "" {
				req.AvatarColor = "#10B981"
			}

			memberObj := &trip.TripMember{
				OrderID:         req.OrderID,
				MemberID:        req.MemberID,
				DisplayName:     req.DisplayName,
				FlatLocation:    req.FlatLocation,
				ItemsSummary:    req.ItemsSummary,
				OrderTotalPaise: req.OrderTotalPaise,
				AvatarColor:     req.AvatarColor,
			}

			t, err := tripStore.JoinTripAtomic(r.Context(), req.TripID, req.OrderID, 3500, memberObj)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(t)
		})

		// Purge any legacy trip keys from Redis on boot to ensure zero stale cached state
		iter := rdb.Scan(context.Background(), 0, "trip:*", 0).Iterator()
		for iter.Next(context.Background()) {
			_ = rdb.Del(context.Background(), iter.Val()).Err()
		}
		_ = rdb.Del(context.Background(), "geofence_trip:geofence-aravali").Err()

		http.HandleFunc("/api/trips/active", func(w http.ResponseWriter, r *http.Request) {
			geofenceID := r.URL.Query().Get("geofence_id")
			if geofenceID == "" {
				geofenceID = "geofence-aravali"
			}
			t, err := tripStore.GetActiveTripForGeofence(r.Context(), geofenceID)
			if err != nil || t == nil {
				t = &trip.Trip{
					ID:                     "trip-rider-v3",
					RiderID:                "rider-1",
					RiderName:              "Rahul Sharma",
					GeofenceID:             geofenceID,
					GeofenceName:           "Aravali Heights, Tower B",
					AssignedOrderCount:     4,
					MemberOrderIDs:         []string{},
					Members:                []trip.TripMember{},
					BaseDeliveryFeePaise:   3500,
					CurrentDeliveryFeePaise: 3500,
					DiscountPaise:          0,
					ETASeconds:             360,
					Status:                 trip.TripStatusAvailable,
					CreatedAt:              time.Now(),
				}
				_ = tripStore.CreateOrUpdateTrip(r.Context(), t)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(t)
		})

		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			
			pingErr := rdb.Ping(r.Context()).Err()
			if pingErr != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status":    "degraded",
					"redis":     "unreachable",
					"error":     pingErr.Error(),
					"timestamp": time.Now(),
				})
				return
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":    "ok",
				"redis":     "connected",
				"timestamp": time.Now(),
			})
		})

		http.Handle("/", http.FileServer(http.Dir("./dashboard")))
		log.Printf("HTTP Server listening continuously on :%d ...", port)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
			log.Fatalf("[FATAL] HTTP server failed: %v", err)
		}
	}()

	runCount := 0

	for {
		runCount++
		if autoRestart {
			log.Printf("\n[SIMULATION LOOP] Starting Run #%d ...", runCount)
		}

		ctxSim, cancelSim := context.WithCancel(context.Background())

		// Initialize stores in a cluster grid
		baseLat, baseLng := 28.6315, 77.2167 // Central Delhi coordinates
		stores := make([]*StoreWrapper, 0, numStores)

		for i := 1; i <= numStores; i++ {
			storeID := fmt.Sprintf("store-%d", i)
			row := (i - 1) / 3
			col := (i - 1) % 3
			lat := baseLat + float64(row)*gridSpacing
			lng := baseLng + float64(col)*gridSpacing

			// Store baseline: capacity = 50, 5 pickers, 1.2 orders/sec base arrival rate, 0.15 services/sec/picker
			s := fulfillment.NewStore(storeID, lat, lng, 50, 1.2, 0.15, 5)
			s.SetPickerParams(fixedSeed+int64(100*i)+int64(runCount*1000), timeScale)
			stores = append(stores, &StoreWrapper{store: s})

			// Pre-populate stock in Redis (1,000 units per store)
			_ = resStore.SetStock(ctxSim, storeID, "SKU-GENERIC", 1000)

			// Pre-populate active Group Carts in Redis for dark store geofences
			geofenceID := fmt.Sprintf("geofence-%s", storeID)
			cartID := fmt.Sprintf("cart-group-%s", storeID)

			activeCart := &groupcart.GroupCart{
				ID:                   cartID,
				GeofenceID:           geofenceID,
				Members:              []groupcart.Member{{ID: fmt.Sprintf("host-%s", storeID), DisplayName: "Host"}},
				TotalPaise:           15000,
				UnlockThresholdPaise: 20000,
				Unlocked:             false,
				Status:               groupcart.CartStatusActive,
			}
			data, _ := json.Marshal(activeCart)
			_ = rdb.Set(ctxSim, fmt.Sprintf("cart:%s", cartID), data, 0).Err()
			_ = rdb.Set(ctxSim, fmt.Sprintf("geofence_cart:%s", geofenceID), cartID, 0).Err()
		}

		// Initialize Phase 5 Rider Simulator with 0.8 km (800m) proximity threshold
		riderSim := trip.NewRiderSimulator(tripStore, 0.8)

		// Register active rider heading toward Store-1 geofence (starts 1.5 km away)
		riderSim.RegisterRider(&trip.Rider{
			ID:                    "rider-1",
			Name:                  "Rahul Sharma",
			CurrentLat:            baseLat - 0.0135, // ~1.5 km away
			CurrentLng:            baseLng,
			PickupLat:             baseLat - 0.0135,
			PickupLng:             baseLng,
			DestinationGeofenceID: "geofence-aravali",
			DestinationLat:        baseLat,
			DestinationLng:        baseLng,
			AssignedOrderIDs:      []string{"ord-z101", "ord-z102", "ord-z103", "ord-z104"},
			Status:                trip.RiderStatusEnRoute,
			SpeedKmH:              30.0, // 30 km/h
		})

		rawStores := make([]*fulfillment.Store, len(stores))
		for i, sw := range stores {
			rawStores[i] = sw.store
		}

		var globalOrderCounter int64

		// Pre-generate 100% deterministic order arrival schedules for each store using fixed seed
		storeSchedules := make(map[string][]ScheduledOrder)
		var totalPreGeneratedOrders int64

		for idx, sw := range stores {
			st := sw.store
			storeSeed := fixedSeed + int64((idx+1)*10000) + int64(runCount*100000)

			arrivalRng := simulation.NewRandomGenerator(storeSeed)
			perishableRng := rand.New(rand.NewSource(storeSeed + 999))
			cartRng := rand.New(rand.NewSource(storeSeed + 888))

			var currentSimOffset time.Duration
			schedules := make([]ScheduledOrder, 0)

			for currentSimOffset < simDuration {
				lambda := st.ArrivalRate
				if surgeStore != "" && st.ID == surgeStore {
					if currentSimOffset >= surgeStart && currentSimOffset < (surgeStart+surgeDuration) {
						lambda = st.ArrivalRate * surgeFactor
					}
				}

				stepDuration := arrivalRng.NextInterArrivalTime(lambda)
				currentSimOffset += stepDuration

				if currentSimOffset >= simDuration {
					break
				}

				isPerishable := perishableRng.Float64() < 0.30

				var memberID, groupCartID, geofenceID string
				if cartRng.Float64() < 0.40 {
					geofenceID = fmt.Sprintf("geofence-%s", st.ID)
					groupCartID = fmt.Sprintf("cart-group-%s", st.ID)
					memberID = fmt.Sprintf("mem-%s-%d", st.ID, cartRng.Intn(10)+1)
				}

				orderNum := atomic.AddInt64(&globalOrderCounter, 1)
				orderID := fmt.Sprintf("ord-%s-%d", st.ID, orderNum)

				ord := &fulfillment.Order{
					ID:              orderID,
					StoreID:         st.ID,
					OriginalStoreID: st.ID,
					MemberID:        memberID,
					GroupCartID:     groupCartID,
					GeofenceID:      geofenceID,
					IsPerishable:    isPerishable,
				}

				schedules = append(schedules, ScheduledOrder{
					SimTimeOffset: currentSimOffset,
					Order:         ord,
				})
				totalPreGeneratedOrders++
			}

			storeSchedules[st.ID] = schedules
		}

		var actualNetworkOrdersEnqueued int64

		costCfg := fulfillment.DefaultCostModelConfig()
		engine := fulfillment.NewDecisionEngine(engineMode, costCfg, cartStore, resStore)

		// Monitor interval scales with timeScale, bounded to a reasonable 20ms..500ms range
		monitorInterval := time.Duration(50.0 / timeScale * float64(time.Millisecond))
		if monitorInterval < 20*time.Millisecond {
			monitorInterval = 20 * time.Millisecond
		}

		monitor := fulfillment.NewLoadMonitor(rawStores, 0.85, 0.70, monitorInterval, timeScale, engine, func() int64 {
			return atomic.LoadInt64(&actualNetworkOrdersEnqueued)
		})

		// Stream decision events to WebSocket clients
		stopEventStream := make(chan bool)
		go func() {
			for {
				select {
				case <-stopEventStream:
					return
				case ev, ok := <-engine.EventStream():
					if !ok {
						return
					}
					opsServer.BroadcastEvent(ev)
				}
			}
		}()

		// Start store workers & monitor
		for _, sw := range stores {
			sw.store.Start(ctxSim)
		}
		monitor.Start(ctxSim)

		// Rider movement ticking background loop
		go func() {
			ticker := time.NewTicker(200 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctxSim.Done():
					return
				case <-ticker.C:
					// Advance rider position by 0.2s * timeScale
					deltaSimSec := 0.2 * timeScale
					events, _ := riderSim.Tick(ctxSim, deltaSimSec)
					for _, ev := range events {
						opsServer.BroadcastTripEvent(ev)
						log.Printf("[RIDER TRIP] SimTime: Proximity Triggered | Rider: %s | Trip: %s | ETA: %.1fs | Fee: ₹%.2f (Discount: ₹%.2f)",
							ev.RiderID, ev.TripID, ev.ETASeconds, float64(ev.CurrentDeliveryFeePaise)/100.0, float64(ev.DiscountPaise)/100.0)
					}
				}
			}
		}()

		startTime := time.Now()
		for _, sw := range stores {
			sw.store.SetStartTime(startTime, timeScale)
		}
		var wg sync.WaitGroup

		// Dispatch pre-scheduled order arrivals driven by precise real-time timer offsets
		for _, sw := range stores {
			wg.Add(1)
			go func(wrapper *StoreWrapper) {
				defer wg.Done()
				st := wrapper.store
				schedules := storeSchedules[st.ID]

				for _, sched := range schedules {
					targetRealOffset := time.Duration(float64(sched.SimTimeOffset) / timeScale)
					elapsedReal := time.Since(startTime)
					waitNeeded := targetRealOffset - elapsedReal

					if waitNeeded > 0 {
						select {
						case <-ctxSim.Done():
							return
						case <-time.After(waitNeeded):
						}
					}

					nowSim := startTime.Add(sched.SimTimeOffset)
					sched.Order.QueuedAt = nowSim

					st.EnqueueOrder(sched.Order)
					st.IncrReceived()
					atomic.AddInt64(&actualNetworkOrdersEnqueued, 1)
				}
			}(sw)
		}

		wg.Wait()

		// Wait out remaining simulation duration to let pickers complete remaining queue
		elapsedReal := time.Since(startTime)
		if realDuration > elapsedReal {
			time.Sleep(realDuration - elapsedReal)
		}

		// Stop workers
		for _, sw := range stores {
			sw.store.Stop()
		}
		monitor.Stop()
		close(stopEventStream)

		// Collect statistics
		fmt.Println("\n=========================================================================================================")
		fmt.Println("                                   SIMULATION NETWORK STORE REPORT                                       ")
		fmt.Println("=========================================================================================================")
		fmt.Printf("%-10s | %-9s | %-9s | %-10s | %-9s | %-9s | %-13s | %-13s\n",
			"Store ID", "Received", "Completed", "ReRouted-In", "ReRouteOut", "In Queue", "Avg Service(s)", "Avg System(s)")
		fmt.Println("---------------------------------------------------------------------------------------------------------")

		var netReceived, netCompleted, netReRoutedIn, netReRoutedOut, netInQueue, netInPicking int64

		for _, sw := range stores {
			rec, comp, rIn, rOut, inQ, inP, avgServiceSec, avgSystemSec := sw.store.Stats()
			netReceived += rec
			netCompleted += comp
			netReRoutedIn += rIn
			netReRoutedOut += rOut
			netInQueue += inQ
			netInPicking += inP

			fmt.Printf("%-10s | %-9d | %-9d | %-10d | %-9d | %-9d | %-13.2f | %-13.2f\n",
				sw.store.ID, rec, comp, rIn, rOut, inQ, avgServiceSec, avgSystemSec)
		}

		fmt.Println("---------------------------------------------------------------------------------------------------------")

		// Print stuck queue stats for surge store if backlogged
		if surgeStore != "" {
			for _, sw := range stores {
				if sw.store.ID == surgeStore {
					minWait, maxWait, avgWait, count := sw.store.StuckQueueStats(sw.store.SimNow())
					fmt.Printf("[BACKLOG] Store %s Backlogged Queue Wait Stats: Count=%d | Min=%.1fs | Max=%.1fs | Avg=%.1fs\n",
						surgeStore, count, minWait, maxWait, avgWait)
				}
			}
		}

		// Decision Engine Summary
		decSummary := engine.GetSummary()
		fmt.Println("\n=========================================================================================================")
		fmt.Println("                                DECISION ENGINE OUTCOME SUMMARY                                          ")
		fmt.Println("=========================================================================================================")
		fmt.Printf("Engine Mode                       : %s\n", engineMode)
		fmt.Printf("Total Monitor Evaluated ticks     : %d\n", decSummary.TotalEvaluations)
		fmt.Printf("Outcome 1: NO_ACTION              : %d\n", decSummary.NoActionCount)
		fmt.Printf("Outcome 2: BATCHING_NUDGE_ISSUED  : %d  (Group Cart Orders Consolidated into Single Passes)\n", decSummary.BatchingNudgeCount)
		fmt.Printf("Outcome 3: RE_ROUTE_REJECTED_COST : %d  (Cost-Gate Blocked Margin-Negative Transfers)\n", decSummary.RejectedOnCostCount)
		fmt.Printf("Outcome 4: RE_ROUTE_EXECUTED      : %d  (Cost-Gate Passed & Stock Reserved)\n", decSummary.ExecutedCount)
		fmt.Printf("Outcome 5: RE_ROUTE_FAILED_NO_STK : %d  (Stock Unavailable at Destination)\n", decSummary.FailedNoStockCount)
		fmt.Printf("Total Individual Orders Merged    : %d\n", decSummary.TotalOrdersMerged)
		fmt.Println("---------------------------------------------------------------------------------------------------------")

		// Network Accounting Invariant Verification
		createdTotal := atomic.LoadInt64(&actualNetworkOrdersEnqueued)
		invSummary, err := fulfillment.VerifyNetworkAccountingInvariant(rawStores, createdTotal, decSummary.TotalOrdersMerged, decSummary.BatchingNudgeCount)
		fmt.Println("\n=========================================================================================================")
		fmt.Println("                              NETWORK ACCOUNTING INVARIANT CHECK                                         ")
		fmt.Println("=========================================================================================================")
		fmt.Printf("Total Orders Created Network-Wide : %d\n", invSummary.TotalCreated)
		fmt.Printf("Total Orders Completed           : %d\n", invSummary.TotalCompleted)
		fmt.Printf("Total Currently In Queue          : %d\n", invSummary.TotalInQueue)
		fmt.Printf("Total Currently In Picking        : %d\n", invSummary.TotalInPicking)
		fmt.Printf("Total Lost / Dropped              : %d\n", invSummary.TotalDropped)
		fmt.Printf("Net Merged Orders Removed        : %d\n", invSummary.TotalMerged)
		fmt.Printf("Active In System Sum              : %d\n", invSummary.ActiveInSystem)

		if err != nil {
			fmt.Printf("Invariant Status                  : FAILED! (%v)\n", err)
		} else {
			fmt.Printf("Invariant Status                  : PASSED (Created == Completed + InQueue + InPicking + NetMerged)\n")
		}
		fmt.Println("=========================================================================================================")

		cancelSim()

		if !autoRestart {
			break
		}

		log.Printf("\n[SIMULATION LOOP] Run #%d completed. Auto-restarting simulation cycle in 3 seconds...", runCount)
		time.Sleep(3 * time.Second)
	}
}
