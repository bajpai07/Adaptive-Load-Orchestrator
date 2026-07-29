package groupcart

import (
	"fmt"
)

type BillSplit struct {
	CartID          string           `json:"cart_id"`
	MemberTotals    map[string]int64 `json:"member_totals"`
	GrandTotalPaise int64            `json:"grand_total_paise"`
}

// ComputeBillSplit computes each member's share based strictly on items they personally added.
// It verifies that sum(member_totals) == grand_total_paise with exact integer precision.
func ComputeBillSplit(cart *GroupCart) (*BillSplit, error) {
	if cart == nil {
		return nil, fmt.Errorf("cart is nil")
	}

	memberTotals := make(map[string]int64)
	for _, m := range cart.Members {
		memberTotals[m.ID] = 0
	}

	var sumPaise int64
	for _, item := range cart.Items {
		memberTotals[item.AddedByMemberID] += item.PricePaise
		sumPaise += item.PricePaise
	}

	if sumPaise != cart.TotalPaise {
		return nil, fmt.Errorf("BILL SPLIT ARITHMETIC ERROR: Sum of item prices (%d paise) != Cart TotalPaise (%d paise)",
			sumPaise, cart.TotalPaise)
	}

	return &BillSplit{
		CartID:          cart.ID,
		MemberTotals:    memberTotals,
		GrandTotalPaise: cart.TotalPaise,
	}, nil
}
