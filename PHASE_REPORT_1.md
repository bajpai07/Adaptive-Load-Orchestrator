# Phase 1 Report — Fulfillment Engine Core (Final Version)

**Project**: Adaptive Load Orchestrator  
**Phase**: Phase 1 — Fulfillment Engine Core  
**Date**: July 29, 2026  

---

## 1. What Was Built

We built the foundational supply-side **Fulfillment Engine** modeling dark-store operations as an M/M/c queueing network in Go:

- **Store Model (`internal/fulfillment/store.go`)**: Encapsulates dark store capacity, coordinates (lat/lng), picker count ($c=5$), shelf capacity ($100$), arrival rate ($\lambda$), exponential picking service rate ($\mu$), current queue, active picking count, and real-time load calculation.
- **Simulation Math (`internal/simulation/generator.go`, `distance.go`)**: Inverse transform sampling generators for memoryless **Poisson order arrivals** and **exponential picker service durations**, alongside **Haversine distance** calculation for geographical store proximity.
- **Order Queue (`internal/fulfillment/queue.go`)**: Goroutine-safe FIFO queue supporting concurrent pushes, pops, extraction of non-perishable orders for re-routing, and waiting-order latency distribution analysis.
- **Picker Pool (`internal/fulfillment/picker.go`)**: $c$ parallel picker goroutines per store popping orders, recording `PickStartedAt` and `CompletedAt` timestamps, holding active picking state, and completing orders according to exponential service time distributions.
- **Load Monitor (`internal/fulfillment/monitor.go`)**: Ticking background worker computing `current_load = queue_length / (picker_count * capacity_factor)` for every store. Tracks **distinct breach episodes** (entering at >85% load, exiting at <70% recovery threshold) and episode durations.
- **In-Process Event Bus & Rate-Limited Re-Router (`internal/fulfillment/eventbus.go`)**: Subscribes to threshold breach events with explicit **30s simulated cooldown** and **70% hysteresis recovery threshold** rules. When triggered, shifts up to $N=3$ non-perishable orders to the nearest store within a 1.5km radius whose load is below 70%.
- **Accounting Integrity Engine (`internal/fulfillment/accounting.go`)**: Enforces runtime invariant verification ($\text{TotalNetworkCreated} = \text{TotalCompleted} + \text{TotalInQueue} + \text{TotalInPicking} + \text{TotalDropped}$) on every monitor tick and at simulation termination.
- **CLI Runner (`cmd/sim/main.go`)**: Configurable simulation binary supporting `--duration`, `--stores`, `--time-scale`, `--surge-store`, `--surge-factor`, `--surge-start`, `--surge-duration`, `--cooldown`, and `--recovery-threshold`.

---

## 2. What Was Measured (Empirical Benchmark Results)

> [!NOTE]
> All figures below were produced by running the compiled simulation binary `sim.exe` built from commit HEAD.

### Scenario A: Baseline Run (Normal Uniform Load, 30m Simulated Time)

**Reproduction Command**:
```powershell
.\sim.exe --duration=30m --stores=8 --time-scale=100.0 --cooldown=30s --recovery-threshold=0.70
```

**Results**:
- **Simulated Duration**: 30 minutes (Real execution time: 18.0s at 100x speedup)
- **Unique Customer Arrivals Created**: 8,471
- **Total Network Orders Completed**: 8,459
- **Orders Currently in Queue**: 12
- **Orders Currently in Picking**: 0
- **Network Throughput**: 16,918.0 orders/hour
- **Network Average Service Time (`avg_service_time`)**: 6.68 seconds
- **Network Average Time in System (`avg_time_in_system`)**: **10.22 seconds**
- **Distinct Breach Episodes**: **0 episodes** (Raw tick breaches: 0)
- **Re-routes Executed**: 0
- **Accounting Invariant Status**: **PASSED** (Created 8,471 == Active 8,471)

**Per-Store Breakdown**:
| Store ID | Cust Arrivals | Re-routed IN | Re-routed OUT | Total Inbound | Completed | In Queue | `avg_service_time` | `avg_time_in_system` |
|---|---|---|---|---|---|---|---|---|
| `store-1` | 1,042 | 0 | 0 | 1,042 | 1,042 | 0 | 6.99s | 9.38s |
| `store-2` | 1,070 | 0 | 0 | 1,070 | 1,070 | 0 | 6.63s | 9.85s |
| `store-3` | 1,081 | 0 | 0 | 1,081 | 1,081 | 0 | 6.43s | 9.83s |
| `store-4` | 1,025 | 0 | 0 | 1,025 | 1,025 | 0 | 6.92s | 10.89s |
| `store-5` | 1,032 | 0 | 0 | 1,032 | 1,032 | 0 | 6.85s | 11.44s |
| `store-6` | 1,063 | 0 | 0 | 1,063 | 1,051 | 12 | 6.75s | 11.24s |
| `store-7` | 1,048 | 0 | 0 | 1,048 | 1,048 | 0 | 6.32s | 8.51s |
| `store-8` | 1,110 | 0 | 0 | 1,110 | 1,110 | 0 | 6.56s | 10.65s |
| **NETWORK** | **8,471** | **0** | **0** | **8,471** | **8,459** | **12** | **6.68s** | **10.22s** |

---

### Scenario B: IPL Match Surge Scenario (Store 1 Surge 3.0x for 10m Window)

**Reproduction Command**:
```powershell
.\sim.exe --duration=30m --stores=8 --time-scale=100.0 --surge-store=store-1 --surge-factor=3.0 --surge-start=5m --surge-duration=10m --cooldown=30s --recovery-threshold=0.70
```

**Results**:
- **Simulated Duration**: 30 minutes (Real execution time: 18.0s)
- **Unique Customer Arrivals Created**: 9,171 (+700 orders vs baseline)
- **Total Network Orders Completed**: 8,721
- **Orders Currently in Queue**: 450 (441 accumulated in `store-1` queue)
- **Orders Currently in Picking**: 0
- **Network Throughput**: 17,442.0 orders/hour
- **Network Average Service Time (`avg_service_time`)**: 6.69 seconds
- **Network Average Time in System (`avg_time_in_system`)**: **65.32 seconds**
- **Distinct Breach Episodes**: **1 episode** (Duration: **1,461.6s** / 24.36 minutes simulated time; Raw debug ticks: 14,414)
- **Re-routes Executed**: **147**
- **Accounting Invariant Status**: **PASSED** (Created 9,171 == Active 9,171)

**Per-Store Breakdown under Surge**:
| Store ID | Cust Arrivals | Re-routed IN | Re-routed OUT | Total Inbound | Completed | In Queue | `avg_service_time` | `avg_time_in_system` |
|---|---|---|---|---|---|---|---|---|
| `store-1` | **1,818** | 0 | **147** | 1,818 | 1,230 | **441** | 6.99s | **355.59s** ($\approx 5.9$m) |
| `store-2` | 1,004 | **147** | 0 | 1,151 | 1,151 | 0 | 6.64s | **63.51s** |
| `store-3` | 1,084 | 0 | 0 | 1,084 | 1,084 | 0 | 6.42s | 8.94s |
| `store-4` | 1,071 | 0 | 0 | 1,071 | 1,071 | 0 | 6.89s | 10.67s |
| `store-5` | 1,022 | 0 | 0 | 1,022 | 1,020 | 2 | 6.87s | 8.76s |
| `store-6` | 1,055 | 0 | 0 | 1,055 | 1,049 | 6 | 6.75s | 9.78s |
| `store-7` | 1,093 | 0 | 0 | 1,093 | 1,092 | 1 | 6.36s | 7.92s |
| `store-8` | 1,024 | 0 | 0 | 1,024 | 1,024 | 0 | 6.60s | 9.99s |
| **NETWORK** | **9,171** | **147** | **147** | **9,318** | **8,721** | **450** | **6.69s** | **65.32s** |

---

### Uncompleted Stuck Queue Wait Distribution (Surge Scenario End)

For orders remaining stuck in store queues at simulation end, their current wait durations (`now - QueuedAt`) are:

| Store ID | Orders Waiting | Min Current Wait (s sim) | Max Current Wait (s sim) | Avg Current Wait (s sim) |
|---|---|---|---|---|
| `store-1` | **441** | **41.2s** | **714.7s** ($\approx 11.9$m) | **381.5s** ($\approx 6.4$m) |
| `store-5` | 2 | 41.5s | 42.4s | 42.0s |
| `store-6` | 6 | 42.1s | 50.0s | 45.4s |
| `store-7` | 1 | 41.5s | 41.5s | 41.5s |

> [!IMPORTANT]
> **Key Finding**: While `avg_service_time` remained flat at ~6.99s across baseline and surge runs, **`avg_time_in_system` degraded from 9.38s to 355.59s** at `store-1` due to queue backlog accumulation.
> This `avg_time_in_system` metric is the explicit SLA customer latency parameter that Phase 3's cost model will consume for penalty calculation $f(\text{predicted\_delay\_minutes})$.

---

## 3. Data Race Verification (`go test -race`)

Execution of Go's race detector across all packages using MinGW GCC 16.1.0:

**Command**:
```powershell
$env:PATH="C:\msys64\mingw64\bin;" + $env:PATH; $env:CGO_ENABLED="1"; go test -count=1 -race -v ./...
```

**Literal Terminal Output**:
```
?   	adaptive-load-orchestrator/cmd/sim	[no test files]
=== RUN   TestAccountingIntegrity_InvariantHoldsUnderConcurrentSurgeAndReRouting
2026/07/29 10:42:34 [NAIVE REROUTE + COOLDOWN] Transferred 3 non-perishable order(s) store-1 -> store-2 (Dist: 1.18 km | SimTime: 111.462ms)
--- PASS: TestAccountingIntegrity_InvariantHoldsUnderConcurrentSurgeAndReRouting (0.61s)
=== RUN   TestFulfillmentEngine_EndToEndNaiveReRouteWithCooldown
2026/07/29 10:42:34 [NAIVE REROUTE + COOLDOWN] Transferred 3 non-perishable order(s) store-1 -> store-2 (Dist: 1.18 km | SimTime: 21.037ms)
--- PASS: TestFulfillmentEngine_EndToEndNaiveReRouteWithCooldown (0.10s)
=== RUN   TestStore_ComputeLoadFormula
--- PASS: TestStore_ComputeLoadFormula (0.00s)
=== RUN   TestLoadMonitor_ThresholdBreachDetection
--- PASS: TestLoadMonitor_ThresholdBreachDetection (0.01s)
=== RUN   TestOrderQueue_ConcurrentPushesAndPops
--- PASS: TestOrderQueue_ConcurrentPushesAndPops (0.01s)
=== RUN   TestOrderQueue_ExtractNonPerishables
--- PASS: TestOrderQueue_ExtractNonPerishables (0.00s)
PASS
ok  	adaptive-load-orchestrator/internal/fulfillment	1.825s
=== RUN   TestPoissonArrivalGenerator_StatisticalProperties
--- PASS: TestPoissonArrivalGenerator_StatisticalProperties (0.10s)
=== RUN   TestExponentialServiceGenerator_StatisticalProperties
--- PASS: TestExponentialServiceGenerator_StatisticalProperties (0.09s)
=== RUN   TestHaversineDistance
--- PASS: TestHaversineDistance (0.00s)
PASS
ok  	adaptive-load-orchestrator/internal/simulation	1.277s
```

**Result**: **Zero data races reported** across all packages.

---

## 4. What Was Simplified or Skipped

- **Aisle Congestion & Pathfinding**: Pickers are modeled as memoryless M/M/c workers. Physical aisle congestion, shelf item distances, and picking routes are omitted.
- **Inventory Stock Reservation**: All stores are assumed to have infinite stock of non-perishable items. Multi-store inventory validation is deferred to Phase 3 (Redis Lua reservation script).
- **Delivery Leg Costs**: Re-routing moves orders without checking delivery leg cost or SLA breach penalty (deferred to Phase 3 cost model).
- **Group Carts & Batching**: Demand-side batching is not present (Phase 2 & Phase 4 scope).
- **In-Memory Event Bus**: Used in-process Go channels rather than Redis streams for Phase 1 performance.

---

## 5. What Would Break This in Production

1. **Capacity Saturation under Sustained Surge**: With a 30s cooldown, `store-1` re-routed 147 orders (49 re-route batches of 3 orders) to `store-2`. However, during a 3x surge, `store-1` received 1,818 customer arrivals vs its picking capacity of 1,230 orders, leaving 441 orders in queue at simulation end with an average wait time of 381.5s ($\approx 6.4$ minutes). Cooldown alone prevents ping-pong churn, but does not increase physical picker capacity.
2. **Unbounded Queue Memory**: `OrderQueue` is an in-memory slice. If arrival rate vastly exceeds total picking capacity ($\lambda \gg c\mu$) for extended periods, memory usage will grow unbounded.
3. **Single-Instance Event Bus**: The in-process Go channel bus cannot span multiple simulation nodes or dark store microservices. Distributed deployment requires Redis Pub/Sub or Streams.
