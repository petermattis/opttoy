package v4

import (
	"math"
)

type optimizePass uint16

const (
	fullyOptimizedPass optimizePass = math.MaxInt16
)

type optimizer struct {
	memo     *memo
	factory  *factory
	coster   *coster
	explorer *explorer
	pass     optimizePass
}

type optRequest struct {
	required physicalPropsID
	maxCost  physicalCost
}

func newOptimizer(memo *memo) *optimizer {
	factory := newFactory(memo)
	coster := newCoster(memo)
	explorer := newExplorer(memo, factory)
	return &optimizer{memo: memo, factory: factory, coster: coster, explorer: explorer}
}

func (o *optimizer) optimize(required physicalPropsID) expr {
	best := o.optimizeGroup(o.memo.getRoot(), required, maxCost)
	return expr{memo: o.memo, group: o.memo.root, op: best.op, offset: best.offset, required: required}
}

func (o *optimizer) optimizeGroup(mgrp *memoGroup, required physicalPropsID, maxCost physicalCost) bestExpr {
	// Check whether this group has already been optimized during the current
	// optimization pass, or if has already been fully optimized.
	if o.isGroupOptimizedThisPass(mgrp) {
		return o.enforceProps(mgrp, required)
	}

	// As long as there's been some improvement, keep optimizing the group.
	iter := 0
	start := 0
	for {
		groupFullyOptimized := true
		groupCostImproved := false

		for index, offset := range mgrp.exprs[start:] {
			// If expression is already fully optimized, then skip it.
			if o.isExprFullyOptimized(mgrp, index) {
				continue
			}

			op := o.memo.lookupExpr(offset).op
			e := expr{memo: o.memo, group: mgrp.id, op: op, offset: offset, required: required}

			recomputeCost, exprFullyOptimized := o.optimizeExpr(&e, maxCost)
			if exprFullyOptimized {
				o.markExprAsFullyOptimized(mgrp, index)
			} else {
				groupFullyOptimized = false
			}

			// If this is the first time that the expression has been costed, then
			// always compute its cost.
			if index >= int(mgrp.optimizeCtx.start) {
				recomputeCost = true
			}

			if recomputeCost && o.recomputeCost(mgrp, &e) {
				groupCostImproved = true
			}
		}

		// Recompute the cost of any enforcers if any expression in the group
		// improved.
		if groupCostImproved {
			o.recomputeEnforcerCost(mgrp)
		}

		iter++
		start = len(mgrp.exprs)
		mgrp.optimizeCtx.start = uint32(start)

		// Now generate new expressions that are logically equivalent to other
		// expressions in this group. Until all expressions have been transitively
		// generated, optimization of this group is not complete.
		if !o.explorer.exploreGroup(mgrp, o.pass, iter) {
			groupFullyOptimized = false
		}

		if groupFullyOptimized {
			// If exploration and costing of this group is complete, then skip it
			// in all future optimization passes.
			mgrp.optimizeCtx.pass = fullyOptimizedPass
			break
		}

		// This group has been optimized during this pass, but there may be
		// further iterations.
		mgrp.optimizeCtx.pass = o.pass

		if !groupCostImproved {
			// The group's cost did not improve, so iterations are complete
			// during this pass
			break
		}
	}

	return o.enforceProps(mgrp, required)
}

func (o *optimizer) optimizeExpr(e *expr, maxCost physicalCost) (recomputeCost, fullyOptimized bool) {
	recomputeCost = false
	fullyOptimized = true

	for child := 0; child < e.childCount(); child++ {
		childGroup := o.memo.lookupGroup(e.childGroup(child))

		// Given required parent properties, get the properties required from
		// the child.
		required := physicalPropsFactory{}.constructRequiredProps(e, child)

		// Recursively optimize the child group.
		best := o.optimizeGroup(childGroup, required, maxCost)

		// If a lower cost expression was found for the child group during this
		// optimization pass, then recompute the cost of the parent expression
		// as well.
		if best.pass == o.pass {
			recomputeCost = true
		}

		// If any child group is not fully optimized, then this expression is
		// also not fully optimized.
		if !o.isGroupFullyOptimized(childGroup) {
			fullyOptimized = false
		}

		// If the child's best cost is greater than the remaining max cost,
		// then there's no way this expression's cost is going to be better, so
		// abandon further work on it or its children, at least during this
		// optimization pass. Permanently abandon work on this expression (i.e.
		// prune it) if all child groups have been optimized up until
		// this point. In that case, further optimization passes will not
		// reduce the cost, so pruning is in order.
		if maxCost.Less(best.cost) {
			// If any child group has been fully optimized, but has a cost
			// that's greater than the total cost budget, then there's no way
			// the expression's cost will ever be good enough.
			if o.isGroupFullyOptimized(childGroup) && maxCost.Less(best.cost) {
				fullyOptimized = true
			}

			// No need to recompute the expression's cost, since it's going to
			// be higher than the budget anyway.
			recomputeCost = false
			break
		}

		// Decrease the remaining max cost by the cost of the child.
		maxCost = maxCost.Sub(best.cost)
	}

	return
}

func (o *optimizer) enforceProps(mgrp *memoGroup, required physicalPropsID) bestExpr {
	best, ok := mgrp.bestExprs[required]
	if ok {
		return best
	}

	// Since there is no expression that provides the required properties, one
	// or more enforcers will be needed.
	providedProps := physicalProps{}
	provided := wildcardPhysPropsID

	requiredProps := o.memo.lookupPhysicalProps(required)
	if requiredProps.order != nil {
		providedProps.order = requiredProps.order
		provided := o.memo.internPhysicalProps(&providedProps)

		e := expr{memo: o.memo, group: mgrp.id, op: sortOp, required: provided}
		cost := o.coster.computeSortCost(&e)

		mgrp.bestExprs[provided] = bestExpr{op: sortOp, pass: o.pass, provided: provided, cost: cost}
	}

	if provided != required {
		fatalf("enforcers did not provide the complete set of required physical properties")
	}

	return best
}

func (o *optimizer) recomputeEnforcerCost(mgrp *memoGroup) {
	for required, best := range mgrp.bestExprs {
		if !best.isEnforcer() {
			continue
		}

		e := expr{memo: o.memo, group: mgrp.id, op: best.op, required: required}
		o.ratchetBestExpr(mgrp, &e, required, best.provided)
	}
}

func (o *optimizer) recomputeCost(mgrp *memoGroup, e *expr) bool {
	// Special-case certain operators.
	switch e.operator() {
	case selectOp:
		return o.ratchetPassthroughBestExprs(mgrp, e, 0)

	case projectOp:
		return o.ratchetPassthroughBestExprs(mgrp, e, 0)

	default:
		return o.ratchetBestExpr(mgrp, e, wildcardPhysPropsID, wildcardPhysPropsID)
	}
}

func (o *optimizer) ratchetPassthroughBestExprs(mgrp *memoGroup, e *expr, child int) bool {
	improved := false

	for required, best := range o.memo.lookupGroup(e.childGroup(child)).bestExprs {
		if best.isEnforcer() {
			// Don't add child enforcers to the parent group, because it's
			// better to add those only when needed, as late as possible.
			continue
		}

		if o.ratchetBestExpr(mgrp, e, required, best.provided) {
			improved = true
		}
	}

	return improved
}

func (o *optimizer) ratchetBestExpr(mgrp *memoGroup, e *expr, required, provided physicalPropsID) bool {
	cost := o.coster.computeCost(e)

	best, ok := mgrp.bestExprs[required]
	if ok && !best.cost.Less(cost) {
		// Existing best expression has a lower cost, so do nothing.
		return false
	}

	best = bestExpr{op: e.op, pass: o.pass, offset: e.offset, provided: provided, cost: cost}
	mgrp.bestExprs[required] = best
	return true
}

// isGroupOptimizedThisPass returns true if the specified memo group has
// already been optimized during this optimization pass, or if all possible
// optimizations have already been applied to this group and all subgroups.
// In that case, there is no need to reevaluate this group.
func (o *optimizer) isGroupOptimizedThisPass(mgrp *memoGroup) bool {
	return mgrp.optimizeCtx.pass < o.pass
}

func (o *optimizer) isGroupFullyOptimized(mgrp *memoGroup) bool {
	return mgrp.optimizeCtx.pass == fullyOptimizedPass
}

func (o *optimizer) isExprFullyOptimized(mgrp *memoGroup, index int) bool {
	return mgrp.optimizeCtx.exprs.Contains(index)
}

func (o *optimizer) markExprAsFullyOptimized(mgrp *memoGroup, index int) {
	mgrp.optimizeCtx.exprs.Add(index)
}
