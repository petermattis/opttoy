package opt

type bestExpr struct {
	// op is the operator type of this expression.
	op Operator

	costedIndex uint32

	// offset is the offset of the lowest cost expression in the memo's arena.
	offset exprOffset

	// lastOptimized is the pass in which this expression was last optimized.
	// A given expression is optimized at most once per optimization pass.
	lastOptimized optimizePass

	// lastImproved is the most recent optimization pass in which a lower cost
	// expression was found.
	lastImproved optimizePass

	// fullyOptimized contains the set of expressions that have been fully
	// optimized for the required properties. These never need to be recosted,
	// no matter how many additional optimization passes are made.
	fullyOptimized bitmap

	// cost estimates how expensive this expression will be to execute.
	cost physicalCost
}

func (be *bestExpr) ratchetCost(e *Expr, cost physicalCost, pass optimizePass) {
	// Overwrite existing best expression if the new cost is lower.
	if cost.Less(be.cost) {
		be.op = e.op
		be.offset = e.offset
		be.lastImproved = pass
		be.cost = cost
	}
}

func (be *bestExpr) isEnforcer() bool {
	return be.offset == 0
}

// wasOptimizedSince returns true if the expression was optimized during or
// after the given optimization pass, or if all possible optimizations have
// already been applied so that the cost will never improve.
func (be *bestExpr) wasOptimizedSince(pass optimizePass) bool {
	return !be.lastOptimized.Less(pass)
}

func (be *bestExpr) isFullyOptimized() bool {
	return be.lastOptimized == fullyOptimizedPass
}

func (be *bestExpr) isExprFullyOptimized(exprIndex int) bool {
	return be.fullyOptimized.Contains(exprIndex)
}

func (be *bestExpr) markExprAsFullyOptimized(exprIndex int) {
	be.fullyOptimized.Add(exprIndex)
}
