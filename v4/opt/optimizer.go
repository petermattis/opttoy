package opt

import (
	"math"
)

type optimizer struct {
	mem      *memo
	maxSteps int
	factory  *Factory
	coster   coster
	explorer explorer
	pass     optimizePass
}

func newOptimizer(factory *Factory, maxSteps int) *optimizer {
	o := &optimizer{mem: factory.mem, maxSteps: maxSteps, factory: factory}
	o.coster.init(factory.mem)
	o.explorer.init(factory)
	return o
}

func (o *optimizer) optimize(root GroupID, required physicalPropsID) Expr {
	mgrp := o.mem.lookupGroup(root)
	best := o.optimizeGroup(mgrp, required, maxCost)
	return Expr{mem: o.mem, group: root, op: best.op, offset: best.offset, required: required}
}

func (o *optimizer) optimizeGroup(mgrp *memoGroup, required physicalPropsID, maxCost physicalCost) *bestExpr {
	// Check whether this group has already been optimized during the current
	// optimization pass, or if has already been fully optimized.
	if o.isGroupOptimizedThisPass(mgrp) {
		return o.enforceProps(mgrp, required)
	}

	// As long as there's been some improvement, keep optimizing the group.
	pass := o.pass
	start := 0
	for {
		groupFullyOptimized := true
		groupCostImproved := false

		for index, offset := range mgrp.exprs[start:] {
			// If expression is already fully optimized, then skip it.
			if o.isExprFullyOptimized(mgrp, index) {
				continue
			}

			op := o.mem.lookupExpr(offset).op
			e := Expr{mem: o.mem, group: mgrp.id, op: op, offset: offset, required: required}

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

		// Recompute the cost of enforcement if any expression in the group
		// improved. Also, check whether substituting enforcers will improve
		// the group cost.
		if groupCostImproved {
			o.recomputeEnforceCost(mgrp)
		}

		pass.minor++
		start = len(mgrp.exprs)
		mgrp.optimizeCtx.start = uint32(start)

		// Now generate new expressions that are logically equivalent to other
		// expressions in this group. Until all expressions have been transitively
		// generated, optimization of this group is not complete.
		if o.maxSteps > 0 {
			if !o.explorer.exploreGroup(mgrp, pass) {
				groupFullyOptimized = false
			}
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

func (o *optimizer) optimizeExpr(e *Expr, maxCost physicalCost) (recomputeCost, fullyOptimized bool) {
	recomputeCost = false
	fullyOptimized = true

	for child := 0; child < e.ChildCount(); child++ {
		childGroup := o.mem.lookupGroup(e.ChildGroup(child))

		// Given required parent properties, get the properties required from
		// the child.
		required := o.mem.physPropsFactory.constructRequiredProps(e, child)

		// Recursively optimize the child group.
		best := o.optimizeGroup(childGroup, required, maxCost)

		// If a lower cost expression was found for the child group during this
		// optimization pass, then recompute the cost of the parent expression
		// as well. Since each parent expression is only costed once during a
		// given pass, this won't trigger redundant work, even if in the case
		// of multiple iterations during the pass.
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

func (o *optimizer) enforceProps(mgrp *memoGroup, required physicalPropsID) *bestExpr {
	// Look for exact match.
	best := mgrp.lookupBestExpr(required)
	if best != nil {
		return best
	}

	// No exact match could be found, so look for the best match. Start by
	// searching for an existing expression that provides all the required
	// properties (but was indexed using a subset of required properties).
	best = o.findLowestCostProvider(mgrp, required)
	if best != nil {
		mgrp.ratchetBestExpr(required, best)
		return best
	}

	// Couldn't find an existing expression that provides all the properties,
	// so look for an expression that provides as many properties as possible.
	// Use this as a starting point towards enforcing the required properties.
	var provided physicalPropsID
	for {
		required = o.stripEnforcedProperty(required)

		best = o.findLowestCostProvider(mgrp, required)
		if best != nil {
			provided = best.provided
			break
		}
	}

	if provided == 0 {
		requiredProps := o.mem.lookupPhysicalProps(required)
		fatalf("no expression in group %d for required properties: %v", mgrp.id, requiredProps)
	}

	return o.addEnforcers(mgrp, required, provided)
}

func (o *optimizer) findLowestCostProvider(mgrp *memoGroup, required physicalPropsID) *bestExpr {
	requiredProps := o.mem.lookupPhysicalProps(required)

	var lowestCost *bestExpr
	for i := range mgrp.bestExprs {
		best := &mgrp.bestExprs[i]
		if lowestCost == nil || best.cost.Less(lowestCost.cost) {
			if o.mem.lookupPhysicalProps(best.provided).Provides(requiredProps) {
				lowestCost = best
			}
		}
	}

	return lowestCost
}

func (o *optimizer) stripEnforcedProperty(required physicalPropsID) physicalPropsID {
	requiredProps := *o.mem.lookupPhysicalProps(required)

	// Always strip projections, since every relational expression can provide
	// projections at no extra cost.
	requiredProps.Projection = Projection{}

	// Strip other required properties in order from least likely to be costly
	// to most likely. This heuristic increases the chance of finding a low
	// cost expression to wrap with enforcer(s).
	if requiredProps.Ordering.Defined() {
		requiredProps.Ordering = Ordering{}
	}

	return o.mem.internPhysicalProps(&requiredProps)
}

func (o *optimizer) addEnforcers(mgrp *memoGroup, required, provided physicalPropsID) *bestExpr {
	requiredProps := *o.mem.lookupPhysicalProps(required)
	providedProps := *o.mem.lookupPhysicalProps(provided)

	// Add additional required enforcers if they aren't already provided.
	if !providedProps.Ordering.Provides(requiredProps.Ordering) {
		providedProps.Ordering = requiredProps.Ordering
		provided = o.mem.internPhysicalProps(&providedProps)

		e := Expr{mem: o.mem, group: mgrp.id, op: SortOp, required: provided}
		o.ratchetBestExpr(mgrp, &e, provided)
	}

	if !providedProps.Provides(&requiredProps) {
		fatalf("enforcers did not provide the complete set of required physical properties")
	}

	// It's possible for the provided properties to be a superset of the
	// required properties. Since best expr lookup needs to be an exact lookup,
	// make sure that the original required props are entered into bestExprs.
	best := mgrp.lookupBestExpr(required)
	if best == nil {
		best = mgrp.lookupBestExpr(provided)
		mgrp.ratchetBestExpr(required, best)
	}

	return best
}

// recomputeEnforceCost scans over the set of best expressions and recomputes
// them as if they used enforcers to add any required properties. Sometimes an
// expression cost will be lower when using an enforcer rather than using an
// expression that naturally provides the properties.
//
// This scan is done after all non-enforcer expressions in the group have been
// updated, and then only if that found a lower cost expression. Since enforcer
// expressions are directly or indirectly dependent on other expressions in the
// group, that could cause their cost to change as well.
func (o *optimizer) recomputeEnforceCost(mgrp *memoGroup) {
	var visited bitmap

	// Start with the lowest cost expression of the default properties.
	best := o.findLowestCostProvider(mgrp, defaultPhysPropsID)

	// Recompute the best exprs by wrapping that expression with enforcers.
	for required, index := range mgrp.bestExprsMap {

		provided := required
		for {
			// Base case of expression that is not dependent on any other
			// expression.
			if provided == defaultPhysPropsID {
				break
			}

			// Base case of visiting a best expression that has already been
			// visited.
			if visited.Contains(index) {
				break
			}

			visited.Add(index)

			// Strip off enforced properties until an already visited
			// expression is found.
			for {
				provided = o.stripEnforcedProperty(provided)

				var ok bool
				index, ok = mgrp.bestExprsMap[provided]
				if ok {
					break
				}

				// If no expression exists yet for the provided properties,
				// just skip to the next.
			}
		}

		// The actual provided properties may be a superset.
		provided = mgrp.lookupBestExpr(provided).provided

		// Add any needed enforcers and ratchet the best expr costs.
		if provided != required {
			o.addEnforcers(mgrp, required, provided)
		}
	}
}

func (o *optimizer) recomputeCost(mgrp *memoGroup, e *Expr) bool {
	if e.IsEnforcer() {
		panic("enforcers should have their cost recomputed in recomputeEnforcerCost")
	}

	// Special-case certain operators.
	switch e.Operator() {
	case SelectOp:
		return o.ratchetPassthruBestExprs(mgrp, e, 0)

	case ProjectOp:
		return o.ratchetPassthruBestExprs(mgrp, e, 0)

	default:
		return o.ratchetBestExpr(mgrp, e, defaultPhysPropsID)
	}
}

func (o *optimizer) ratchetPassthruBestExprs(mgrp *memoGroup, e *Expr, child int) bool {
	improved := false

	childGroup := o.mem.lookupGroup(e.ChildGroup(child))
	for required, index := range childGroup.bestExprsMap {
		best := &childGroup.bestExprs[index]

		if best.isEnforcer() {
			// Don't add child enforcers to the parent group, because it's
			// better to add those at a higher level of the expression tree.
			continue
		}

		if o.ratchetBestExpr(mgrp, e, best.provided) {
			improved = true
		}
	}

	return improved
}

func (o *optimizer) ratchetBestExpr(mgrp *memoGroup, e *Expr, provided physicalPropsID) bool {
	cost := o.coster.computeCost(e)
	best := bestExpr{op: e.op, pass: o.pass, offset: e.offset, provided: provided, cost: cost}
	return mgrp.ratchetBestExpr(e.required, &best)
}

// isGroupOptimizedThisPass returns true if the specified memo group has
// already been optimized during this optimization pass, or if all possible
// optimizations have already been applied to this group and all subgroups.
// In that case, there is no need to reevaluate this group.
func (o *optimizer) isGroupOptimizedThisPass(mgrp *memoGroup) bool {
	return mgrp.optimizeCtx.pass.Less(o.pass)
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
