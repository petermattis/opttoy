package opt

type coster struct {
	mem *memo
}

func (c *coster) init(mem *memo) {
	c.mem = mem
}

func (c *coster) computeCost(e *Expr) physicalCost {
	if e.IsRelational() {
		switch e.Operator() {
		case SortOp:
			return c.computeSortCost(e)
		}
	}

	// By default, cost of parent is sum of child costs.
	return c.computeChildrenCost(e)
}

func (c *coster) computeSortCost(e *Expr) physicalCost {
	return 100 + c.computeChildrenCost(e)
}

func (c *coster) computeArrangeCost(e *Expr) physicalCost {
	return c.computeChildrenCost(e)
}

func (c *coster) computeChildrenCost(e *Expr) physicalCost {
	var cost physicalCost

	for i := 0; i < e.ChildCount(); i++ {
		mgrp := c.mem.lookupGroup(e.ChildGroup(i))
		required := c.mem.physPropsFactory.constructRequiredProps(e, i)
		cost += mgrp.lookupBestExpr(required).cost
	}

	return cost
}
