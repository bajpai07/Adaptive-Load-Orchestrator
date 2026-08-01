package fulfillment

import (
	"fmt"
	"math"
	"strings"
	"sync"
)

// ProductCatalogItem represents a product SKU with inventory and scoring attributes.
type ProductCatalogItem struct {
	SKU           string `json:"sku"`
	Name          string `json:"name"`
	Category      string `json:"category"`
	Brand         string `json:"brand"`
	PricePaise    int64  `json:"price_paise"`
	StockQuantity int    `json:"stock_quantity"`
}

// SubstitutionScoreResult holds the detailed explainable score breakdown.
type SubstitutionScoreResult struct {
	TargetSKU          string              `json:"target_sku"`
	TargetName         string              `json:"target_name"`
	Substitute         *ProductCatalogItem `json:"substitute"`
	TotalScore         float64             `json:"total_score"`
	CategoryScore      float64             `json:"category_score"`
	PriceScore         float64             `json:"price_score"`
	BrandScore         float64             `json:"brand_score"`
	ExplanationReason  string              `json:"explanation_reason"`
}

// SubstitutionEngine manages inventory and transparent substitute selection.
type SubstitutionEngine struct {
	mu            sync.RWMutex
	catalog       map[string]*ProductCatalogItem
	stockoutCount int64
	acceptCount   int64
	rejectCount   int64
}

// NewSubstitutionEngine initializes catalog with default stock levels.
func NewSubstitutionEngine() *SubstitutionEngine {
	engine := &SubstitutionEngine{
		catalog: make(map[string]*ProductCatalogItem),
	}

	// Initialize standard Zepto catalog items
	items := []*ProductCatalogItem{
		// Main Grid Items
		{SKU: "sku-milk", Name: "Amul Taaza Milk (1L)", Category: "fresh-dairy", Brand: "Amul", PricePaise: 7500, StockQuantity: 15},
		{SKU: "sku-eggs", Name: "Farm Fresh Eggs (6-pack)", Category: "fresh-dairy", Brand: "Farm Fresh", PricePaise: 6500, StockQuantity: 0}, // Default out-of-stock for demo
		{SKU: "sku-chips", Name: "Lay's Magic Masala (52g)", Category: "snacks-drinks", Brand: "Lay's", PricePaise: 2000, StockQuantity: 20},
		{SKU: "sku-dahi", Name: "Amul Masti Dahi (400g)", Category: "fresh-dairy", Brand: "Amul", PricePaise: 4500, StockQuantity: 0}, // Default out-of-stock for demo
		{SKU: "sku-coke", Name: "Coca-Cola Bottle (750ml)", Category: "snacks-drinks", Brand: "Coca-Cola", PricePaise: 4000, StockQuantity: 18},

		// Optimal Substitute Candidates
		{SKU: "sku-eggs-sub", Name: "Organic Country Eggs (6-pack)", Category: "fresh-dairy", Brand: "Farm Fresh", PricePaise: 7500, StockQuantity: 25},
		{SKU: "sku-dahi-sub", Name: "Mother Dairy Classic Dahi (400g)", Category: "fresh-dairy", Brand: "Mother Dairy", PricePaise: 5000, StockQuantity: 30},
		{SKU: "sku-coke-sub", Name: "Pepsi Soft Drink Bottle (750ml)", Category: "snacks-drinks", Brand: "Pepsi", PricePaise: 4000, StockQuantity: 22},
		{SKU: "sku-milk-sub", Name: "Mother Dairy Toned Milk (1L)", Category: "fresh-dairy", Brand: "Mother Dairy", PricePaise: 7500, StockQuantity: 40},
		{SKU: "sku-chips-sub", Name: "Kurkure Masala Munch (85g)", Category: "snacks-drinks", Brand: "Kurkure", PricePaise: 2000, StockQuantity: 35},
	}

	for _, item := range items {
		engine.catalog[item.SKU] = item
		// Also index by name for fast lookup
		engine.catalog[item.Name] = item
	}

	// Initial stockout event count for default 0-stock items
	engine.stockoutCount = 2

	return engine
}

// GetCatalog returns all catalog items with stock status.
func (se *SubstitutionEngine) GetCatalog() []*ProductCatalogItem {
	se.mu.RLock()
	defer se.mu.RUnlock()

	seen := make(map[string]bool)
	list := make([]*ProductCatalogItem, 0, len(se.catalog))
	for _, item := range se.catalog {
		if !seen[item.SKU] {
			seen[item.SKU] = true
			list = append(list, item)
		}
	}
	return list
}

// GetItem returns a catalog item by SKU or Name.
func (se *SubstitutionEngine) GetItem(identifier string) (*ProductCatalogItem, bool) {
	se.mu.RLock()
	defer se.mu.RUnlock()

	item, exists := se.catalog[identifier]
	return item, exists
}

// UpdateStock sets the stock_quantity for a SKU or Name and increments stockout counter if set to 0.
func (se *SubstitutionEngine) UpdateStock(identifier string, stock int) (*ProductCatalogItem, bool) {
	se.mu.Lock()
	defer se.mu.Unlock()

	item, exists := se.catalog[identifier]
	if !exists {
		return nil, false
	}

	wasOutOfStock := item.StockQuantity == 0
	item.StockQuantity = stock

	if stock == 0 && !wasOutOfStock {
		se.stockoutCount++
	}

	return item, true
}

// ScoreSubstitute scores a candidate against a target using a transparent 100-point formula:
// 1. Category Match (40 pts)
// 2. Price Proximity (30 pts)
// 3. Brand Match (30 pts)
func ScoreSubstitute(target *ProductCatalogItem, candidate *ProductCatalogItem) SubstitutionScoreResult {
	res := SubstitutionScoreResult{
		TargetSKU:  target.SKU,
		TargetName: target.Name,
		Substitute: candidate,
	}

	if candidate.StockQuantity <= 0 || candidate.SKU == target.SKU || candidate.Name == target.Name {
		res.ExplanationReason = "Candidate out of stock or identical to target"
		return res
	}

	// 1. Category Match (40 pts)
	if strings.EqualFold(candidate.Category, target.Category) {
		res.CategoryScore = 40.0
	}

	// 2. Price Proximity (30 pts)
	priceDelta := math.Abs(float64(candidate.PricePaise - target.PricePaise))
	maxPrice := math.Max(float64(candidate.PricePaise), float64(target.PricePaise))
	if maxPrice > 0 {
		res.PriceScore = 30.0 * (1.0 - (priceDelta / maxPrice))
		if res.PriceScore < 0 {
			res.PriceScore = 0
		}
	}

	// 3. Brand Similarity (30 pts)
	if strings.EqualFold(candidate.Brand, target.Brand) {
		res.BrandScore = 30.0
	} else if res.CategoryScore > 0 {
		// Same category bonus for alternative quality brands
		res.BrandScore = 15.0
	}

	res.TotalScore = math.Round((res.CategoryScore+res.PriceScore+res.BrandScore)*10) / 10

	res.ExplanationReason = fmt.Sprintf("Scored %.1f/100: Category Match (%.0f/40), Price Proximity (%.1f/30), Brand Similarity (%.0f/30)",
		res.TotalScore, res.CategoryScore, res.PriceScore, res.BrandScore)

	return res
}

// FindBestSubstitute evaluates all catalog candidates for a stocked-out item.
func (se *SubstitutionEngine) FindBestSubstitute(identifier string) (*SubstitutionScoreResult, bool) {
	se.mu.RLock()
	defer se.mu.RUnlock()

	target, exists := se.catalog[identifier]
	if !exists {
		return nil, false
	}

	var bestMatch *SubstitutionScoreResult
	seenSKUs := make(map[string]bool)

	for _, cand := range se.catalog {
		if seenSKUs[cand.SKU] {
			continue
		}
		seenSKUs[cand.SKU] = true

		if cand.StockQuantity <= 0 || cand.SKU == target.SKU || cand.Name == target.Name {
			continue
		}

		scoreRes := ScoreSubstitute(target, cand)
		if bestMatch == nil || scoreRes.TotalScore > bestMatch.TotalScore {
			match := scoreRes
			bestMatch = &match
		}
	}

	if bestMatch == nil {
		return nil, false
	}

	return bestMatch, true
}

// RecordAction increments accept/reject counters for metrics calculation.
func (se *SubstitutionEngine) RecordAction(action string) (int64, int64, float64) {
	se.mu.Lock()
	defer se.mu.Unlock()

	if action == "accept" {
		se.acceptCount++
	} else if action == "reject" {
		se.rejectCount++
	}

	totalActions := se.acceptCount + se.rejectCount
	var acceptRate float64
	if totalActions > 0 {
		acceptRate = (float64(se.acceptCount) / float64(totalActions)) * 100.0
		acceptRate = math.Round(acceptRate*10) / 10
	}

	return se.stockoutCount, se.acceptCount, acceptRate
}

// GetMetrics returns live counters for Ops Console.
func (se *SubstitutionEngine) GetMetrics() map[string]interface{} {
	se.mu.RLock()
	defer se.mu.RUnlock()

	totalActions := se.acceptCount + se.rejectCount
	var acceptRate float64
	if totalActions > 0 {
		acceptRate = (float64(se.acceptCount) / float64(totalActions)) * 100.0
		acceptRate = math.Round(acceptRate*10) / 10
	}

	return map[string]interface{}{
		"stockout_events_today":    se.stockoutCount,
		"substitution_accept_count": se.acceptCount,
		"substitution_reject_count": se.rejectCount,
		"accept_rate_pct":          acceptRate,
	}
}
