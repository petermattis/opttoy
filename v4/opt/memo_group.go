package opt

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

type bitmap = util.FastIntSet

// GroupID identifies a memo group. Groups have numbers greater than 0; a
// GroupID of 0 indicates an empty expression or an unknown group.
type GroupID uint32

// memoGroup stores a set of logically equivalent expressions. See the comments
// on memoExpr for the definition of logical equivalency.
type memoGroup struct {
	// ID (a.k.a. index) of the group within the memo.
	id GroupID

	// logical is the set of logical properties that all memo expressions in
	// the group share.
	logical *LogicalProps

	// Offset of the canonical, normalized representation of this expression.
	// This is used by the normalizer to construct normalized expression trees
	// from the bottom up.
	norm exprOffset

	// Set of logically equivalent expressions that are part of the group.
	exprs []exprOffset

	// bestExprs remembers the lowest cost expression that provides a
	// particular set of physical properties.
	bestExprsMap map[physicalPropsID]int
	bestExprs []bestExpr

	// optimizeCtx is used by the optimizer to store intermediate state so that
	// redundant work is minimized. Other classes should not access this state.
	optimizeCtx struct {
		pass  optimizePass
		exprs bitmap
		start uint32
	}

	// exploreCtx is used by the explorer to store intermediate state so that
	// redundant work is minimized. Other classes should not access this state.
	exploreCtx struct {
		pass  optimizePass
		iter  int
		exprs bitmap
		start uint32
		end   uint32
	}
}

func (g *memoGroup) addExpr(offset exprOffset) {
	g.exprs = append(g.exprs, offset)
}

func (g *memoGroup) ratchetBestExpr(required physicalPropsID, best *bestExpr) bool {
	index, ok := g.bestExprsMap[required]
	if ok {
		// Overwrite existing best expression if the new cost is lower.
		if best.cost.Less(g.bestExprs[index].cost) {
			g.bestExprs[index] = *best
			return true
		}

		return false
	}

	// Add new best expression.
	index = len(g.bestExprs)
	g.bestExprs = append(g.bestExprs, *best)
	g.bestExprsMap[required] = index
	return false
}

func (g *memoGroup) lookupBestExpr(required physicalPropsID) *bestExpr {
	index, ok := g.bestExprsMap[required]
	if !ok {
		return nil
	}

	return &g.bestExprs[index]
}

type bestExpr struct {
	// op is the operator type of this expression.
	op Operator

	// pass is the optimization pass in which this lowest cost expression was
	// found and entered into the bestExprs map.
	pass optimizePass

	// offset is the offset of the lowest cost expression in the memo's arena.
	offset exprOffset

	// provided are the physical properties that this expression supplies
	// either directly or indirectly (e.g. pass-through properties). These may
	// be a superset of the required properties in the bestExprs map.
	provided physicalPropsID

	// cost estimates how expensive this expression will be to execute.
	cost physicalCost
}

func (be *bestExpr) isEnforcer() bool {
	return be.offset == 0
}
