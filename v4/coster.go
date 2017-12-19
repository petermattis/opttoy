package v4

type coster struct {
	memo *memo
}

func newCoster(memo *memo) *coster {
	return &coster{memo: memo}
}

func (c *coster) computeCost(e *expr) physicalCost {
	switch e.operator() {
	case sortOp:
		return c.computeSortCost(e)
	}

	// By default, cost of parent is sum of child costs.
	return c.computeChildrenCost(e)
}

func (c *coster) computeSortCost(e *expr) physicalCost {
	return 100 + c.computeChildrenCost(e)
}

func (c *coster) computeChildrenCost(e *expr) physicalCost {
	var cost physicalCost

	for i := 0; i < e.childCount(); i++ {
		mgrp := e.memo.lookupGroup(e.childGroup(i))
		required := physicalPropsFactory{}.constructRequiredProps(e, i)
		cost += mgrp.bestExprs[required].cost
	}

	return cost
}
