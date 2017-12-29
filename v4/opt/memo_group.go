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
	existing := g.ensureBestExpr(required)

	// Overwrite existing best expression if the new cost is lower.
	if best.cost.Less(existing.cost) {
		*existing = *best
		return true
	}

	return false
}

func (g *memoGroup) lookupBestExpr(required physicalPropsID) *bestExpr {
	index, ok := g.bestExprsMap[required]
	if !ok {
		return nil
	}

	return &g.bestExprs[index]
}

func (g *memoGroup) ensureBestExpr(required physicalPropsID) *bestExpr {
	best := g.lookupBestExpr(required)
	if best == nil {
		// Add new best expression.
		index := len(g.bestExprs)
		g.bestExprs = append(g.bestExprs, bestExpr{cost: maxCost})
		g.bestExprsMap[required] = index
		best = &g.bestExprs[index]
	}

	return best
}
