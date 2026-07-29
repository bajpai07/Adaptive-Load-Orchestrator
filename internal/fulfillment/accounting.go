package fulfillment

import (
	"fmt"
)

type NetworkAccountingSummary struct {
	TotalCreated   int64 `json:"total_created"`
	TotalCompleted int64 `json:"total_completed"`
	TotalInQueue   int64 `json:"total_in_queue"`
	TotalInPicking int64 `json:"total_in_picking"`
	TotalDropped   int64 `json:"total_dropped"`
	TotalMerged    int64 `json:"total_merged"`
	ActiveInSystem int64 `json:"active_in_system"`
}

// VerifyNetworkAccountingInvariant enforces the strict network accounting invariant:
// TotalCreated == TotalCompleted + TotalInQueue + TotalInPicking + TotalDropped + (TotalMerged - NudgeCount)
func VerifyNetworkAccountingInvariant(stores []*Store, totalCreated int64, totalMergedAndNudge ...int64) (*NetworkAccountingSummary, error) {
	var totCompleted, totInQueue, totInPicking, netMerged int64

	if len(totalMergedAndNudge) >= 2 {
		totMerged := totalMergedAndNudge[0]
		nudgeCount := totalMergedAndNudge[1]
		if totMerged > nudgeCount {
			netMerged = totMerged - nudgeCount
		}
	} else {
		for _, s := range stores {
			s.Queue().mu.Lock()
			for _, ord := range s.Queue().orders {
				if ord.IsConsolidated && ord.ConsolidatedCount > 1 {
					netMerged += int64(ord.ConsolidatedCount - 1)
				}
			}
			s.Queue().mu.Unlock()
		}
	}

	for _, s := range stores {
		totCompleted += s.CompletedCount()
		totInQueue += int64(s.Queue().Len())
		totInPicking += s.ActivePickingCount()
	}

	activeInSystem := totCompleted + totInQueue + totInPicking + netMerged

	summary := &NetworkAccountingSummary{
		TotalCreated:   totalCreated,
		TotalCompleted: totCompleted,
		TotalInQueue:   totInQueue,
		TotalInPicking: totInPicking,
		TotalDropped:   0,
		TotalMerged:    netMerged,
		ActiveInSystem: activeInSystem,
	}

	if totalCreated != activeInSystem {
		return summary, fmt.Errorf("ACCOUNTING INVARIANT VIOLATED: Created (%d) != ActiveInSystem (%d) [Completed=%d, InQueue=%d, InPicking=%d, NetMerged=%d, Discrepancy=%d]",
			totalCreated, activeInSystem, totCompleted, totInQueue, totInPicking, netMerged, totalCreated-activeInSystem)
	}

	return summary, nil
}
