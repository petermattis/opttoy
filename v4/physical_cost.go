package v4

import (
	"math"
)

type physicalCost float64

const (
	maxCost physicalCost = math.MaxFloat64
)

func (c physicalCost) Less(other physicalCost) bool {
	return c < other
}

func (c physicalCost) Sub(other physicalCost) physicalCost {
	return c - other
}
