package opt

// # Optimization
// Memo groups are recursively optimized according to a set of required
// physical properties. The same group can be (and sometimes is) optimized
// multiple times with different required props. Optimization of a group
// proceeds in two phases:
//
// 1. Compute the cost of any previously generated expressions. That set
//    initially contains only the group's normalized expression, but
//    exploration may yield additional expressions. Costing a parent expression
//    requires that the children first be costed, so costing triggers a
//    recursive traversal of the memo groups.
//
// 2. Invoke the explorer to generate new equivalent expressions for the group.
//    Those new expressions are costed once the optimizer loops back to the
//    first phase.
//
// # Search Algorithm
// The optimizer proceeds in multiple iterative "passes", until either it hits
// some configured limit, or until an exhaustive search is complete. As long as
// the search is allowed to complete, the best plan will be found, just as in
// Volcano and Cascades. The optimizer uses several techniques to maximize the
// chance that it finds the best plan early on:
//
// 1. As with Cascades, the search is highly directed, interleaving exploration
//    with costing in order to prune parts of the tree that cannot yield a
//    better plan. This contrasts with Volcano, which first generates all
//    possible plans in one global phase (exploration), and then determines the
//    lowest cost plan in another global phase (costing).
//
// 2. The optimizer uses a simple hill climbing heuristic to make greedy
//    progress towards the best plan. During a given pass, the optimizer visits
//    each group and performs costing and exploration for that group. As long
//    as doing that yields a lower cost expression for the group, the optimizer
//    will repeat those steps. This finds a local maxima for each group during
//    the current pass.
//
// # Search Space Pruning
// In order to avoid costing or exploring parts of the search space that cannot
// yield a better plan, the optimizer performs aggressive "branch and bound
// pruning". Each group expression is optimized with respect to a "costLimit"
// parameter. As soon as this limit is exceeded, optimization of that
// expression terminates. It's not uncommon for large sections of the search
// space to never be costed or explored due to this pruning. Example:
//
//   innerJoin
//     left:  cost = 50
//     right: cost = 75
//     on:    cost = 25
//
// If the current best expression for the group has a cost of 100, then the
// optimizer does not need to cost or explore the "on" child of the join, and
// does not need to cost the join itself. This is because the combined cost of
// the left and right children already exceeds 100.

import (
	"math"
)

type optimizer struct {
	mem      *memo
	factory  *Factory
	coster   coster
	explorer explorer
	pass     optimizePass
}

func newOptimizer(factory *Factory) *optimizer {
	o := &optimizer{mem: factory.mem, factory: factory, pass: optimizePass{major: 1}}
	o.coster.init(factory.mem)
	o.explorer.init(factory)
	return o
}

func (o *optimizer) optimize(root GroupID, required physicalPropsID) Expr {
	mgrp := o.mem.lookupGroup(root)
	best := o.optimizeGroup(mgrp, required, maxCost)
	if best.op == UnknownOp {
		panic("optimization step returned invalid result")
	}
	return Expr{mem: o.mem, loc: best.loc, op: best.op, required: required}
}

func (o *optimizer) optimizeGroup(mgrp *memoGroup, required physicalPropsID, costLimit physicalCost) *bestExpr {
	// If this group was already optimized during this pass for the given
	// required properties, or if it's already fully optimized, then return
	// the already prepared best expression.
	best := mgrp.ensureBestExpr(required)
	if best.wasOptimizedSince(o.pass) {
		return best
	}

	// As long as there's been some improvement to the best expression, then
	// keep optimizing the group.
	pass := o.pass
	start := exprID(0)
	for {
		groupFullyOptimized := true

		for i := range mgrp.exprs[start:] {
			eid := exprID(i)

			// If the group is already fully optimized for the given required
			// properties, then skip it, since it won't get better.
			if best.isExprFullyOptimized(eid) {
				continue
			}

			// Lower the cost limit further if an expression with a lower cost
			// has already been discovered.
			if best.cost.Less(costLimit) {
				costLimit = best.cost
			}

			// If this is the first time that the expression has been costed, then
			// always compute its cost.
			recomputeCost := eid >= best.costedID

			// Optimize the expression, adding enforcers as necessary to
			// provide the required properties. The best expression will be
			// updated by optimizeExpr if the expression has a lower cost.
			best = o.optimizeExpr(mgrp, eid, required, costLimit, recomputeCost)

			if !best.isExprFullyOptimized(eid) {
				groupFullyOptimized = false
			}
		}

		pass.minor++
		start = exprID(len(mgrp.exprs))
		best.costedID = start

		// Now generate new expressions that are logically equivalent to other
		// expressions in this group.
		if false { //o.factory.maxSteps > 0 {
			if !o.explorer.exploreGroup(mgrp, pass) {
				groupFullyOptimized = false
			}
		}

		if groupFullyOptimized {
			// If exploration and costing of this group for the given required
			// properties is complete, then skip it in all future optimization
			// passes.
			best.lastOptimized = fullyOptimizedPass
			break
		}

		// This group has been optimized during this pass for the given
		// required properties, but there may be further iterations.
		best.lastOptimized = pass

		if best.lastImproved.Less(pass) {
			// The best expression did not improve, so iterations are complete
			// during this pass
			break
		}
	}

	return best
}

func (o *optimizer) optimizeExpr(
	mgrp *memoGroup,
	eid exprID,
	required physicalPropsID,
	costLimit physicalCost,
	recomputeCost bool,
) (best *bestExpr) {
	loc := memoLoc{group: mgrp.id, expr: eid}
	op := mgrp.lookupExpr(eid).op
	e := Expr{mem: o.mem, loc: loc, op: op, required: required}

	// Compute the cost for enforcers to provide the required properties. This
	// may be lower than the expression providing the properties itself.
	fullyEnforced := o.enforceProps(&e, recomputeCost)

	// If the expression cannot provide the required properties, then don't
	// continue.
	if !o.mem.physPropsFactory.canProvide(&e) {
		// If enforcers have been fully costed, then optimization of this
		// expression is complete for the required properties.
		best = mgrp.lookupBestExpr(required)
		if fullyEnforced {
			best.markExprAsFullyOptimized(eid)
		}
		return best
	}

	remainingCost := costLimit
	fullyOptimized := true

	for child := 0; child < e.ChildCount(); child++ {
		childGroup := o.mem.lookupGroup(e.ChildGroup(child))

		// Given required parent properties, get the properties required from
		// the child.
		required := o.mem.physPropsFactory.constructChildProps(&e, child)

		// Recursively optimize the child group.
		bestChild := o.optimizeGroup(childGroup, required, remainingCost)

		// If a lower cost expression was found for the child group during this
		// optimization pass, then recompute the cost of the parent expression
		// as well. Since each parent expression is only costed once during a
		// given pass, this won't trigger redundant work, even if in the case
		// of multiple iterations during the pass.
		if bestChild.lastImproved == o.pass {
			recomputeCost = true
		}

		// If any child expression is not fully optimized, then the parent
		// expression is also not fully optimized.
		if !bestChild.isFullyOptimized() {
			fullyOptimized = false
		}

		// If the child's best cost is greater than the remaining max cost,
		// then there's no way this expression's cost is going to be better, so
		// abandon further work on it or its children, at least during this
		// optimization pass. Permanently abandon work on this expression (i.e.
		// prune it) if all child groups have been optimized up until
		// this point. In that case, further optimization passes will not
		// reduce the cost, so pruning is in order.
		if remainingCost.Less(bestChild.cost) {
			// If any child group has been fully optimized, but has a cost
			// that's greater than the total cost budget, then there's no way
			// the expression's cost will ever be good enough.
			if bestChild.isFullyOptimized() && !bestChild.cost.Less(costLimit) {
				fullyOptimized = true
			}

			// No need to recompute the expression's cost, since it's going to
			// be higher than the budget anyway.
			recomputeCost = false
			break
		}

		// Decrease the remaining max cost by the cost of the child.
		remainingCost = remainingCost.Sub(bestChild.cost)
	}

	// Don't attempt to hoist this lookup above the call to enforceProps,
	// because it recursively calls optimizeGroup, and that can change the
	// address of the best expression when the bestExprs array resizes.
	best = mgrp.lookupBestExpr(required)

	if recomputeCost {
		o.ratchetCost(best, &e)
	}

	if fullyEnforced && fullyOptimized {
		best.markExprAsFullyOptimized(eid)
	}

	return
}

func (o *optimizer) enforceProps(e *Expr, recomputeCost bool) (fullyOptimized bool) {
	props := o.mem.lookupPhysicalProps(e.required)
	innerProps := *props

	// Strip off one property that can be enforced. Other properties will be
	// stripped by recursively optimizing the group with successively fewer
	// properties.
	if props.Projection.Defined() {
		innerProps.Projection = Projection{}

		// Projection costs so little, that if this is the only required
		// property, and the expression can provide it, don't waste time
		// costing the enforcer separately.
		if o.mem.physPropsFactory.canProvideProjection(e) && !innerProps.Defined() {
			return true
		}
	} else if props.Ordering.Defined() {
		innerProps.Ordering = nil
	} else {
		// No remaining properties, so no more enforcers.
		if props.Defined() {
			fatalf("unhandled physical property: %v", props)
		}
		return
	}

	// Optimize the group for the "inner" properties.
	mgrp := o.mem.lookupGroup(e.loc.group)
	inner := *e
	inner.required = o.mem.internPhysicalProps(&innerProps)
	costLimit := mgrp.ensureBestExpr(inner.required).cost
	innerBest := o.optimizeGroup(mgrp, inner.required, costLimit)
	fullyOptimized = innerBest.isFullyOptimized()

	// If a lower cost expression was found for the inner expression during
	// this optimization pass, then recompute the cost of the enforcer as well.
	if innerBest.lastImproved == o.pass {
		recomputeCost = true
	}

	if recomputeCost {
		// Cost the expression with the enforcer added.
		var enforcer Expr
		if props.Projection.Defined() {
			enforcer = Expr{mem: o.mem, loc: memoLoc{group: e.loc.group}, op: ArrangeOp, required: e.required}
		} else if props.Ordering.Defined() {
			enforcer = Expr{mem: o.mem, loc: memoLoc{group: e.loc.group}, op: SortOp, required: e.required}
		}

		// Don't attempt to hoist the lookup above the optimizeGroup call,
		// since that will change the address of the best expression when the
		// bestExprs array resizes.
		best := o.mem.lookupGroup(e.loc.group).lookupBestExpr(e.required)
		o.ratchetCost(best, &enforcer)
	}

	return
}

func (o *optimizer) ratchetCost(best *bestExpr, e *Expr) {
	cost := o.coster.computeCost(e)
	best.ratchetCost(e, cost, o.pass)
}

type optimizePass struct {
	major uint16
	minor uint16
}

var fullyOptimizedPass = optimizePass{major: math.MaxInt16, minor: math.MaxInt16}

func (p optimizePass) Less(other optimizePass) bool {
	if p.major < other.major {
		return true
	}

	if p.major == other.major {
		return p.minor < other.minor
	}

	return false
}
