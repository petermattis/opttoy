package opt

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

type bitmap = util.FastIntSet

// GroupID identifies a memo group. Groups have numbers greater than 0; a
// GroupID of 0 indicates an unknown group.
type GroupID uint32

// ExprID is the index of an expression within its group. ExprID = 0 is always
// the normalized expression for the group.
type exprID uint32

const (
	// normExprID is the index of the group's normalized expression.
	normExprID exprID = 0
)

// memoGroup stores a set of logically equivalent expressions. See the comments
// on memoExpr for the definition of logical equivalency.
type memoGroup struct {
	// ID (a.k.a. index) of the group within the memo.
	id GroupID

	// logical is the set of logical properties that all memo expressions in
	// the group share.
	logical *LogicalProps

	// Set of logically equivalent expressions that are part of the group. The
	// first expression is always the group's normalized expression.
	exprs []memoExpr

	// bestExprs remembers the lowest cost expression that provides a
	// particular set of physical properties.
	bestExprsMap map[physicalPropsID]int
	bestExprs    []bestExpr

	// exploreCtx is used by the explorer to store intermediate state so that
	// redundant work is minimized. Other classes should not access this state.
	exploreCtx struct {
		pass  optimizePass
		exprs bitmap
		start exprID
		end   exprID
	}
}

// addExpr appends a new expression to the existing group and returns its id.
func (g *memoGroup) addExpr(mexpr *memoExpr) exprID {
	g.exprs = append(g.exprs, *mexpr)
	return exprID(len(g.exprs) - 1)
}

// lookupExpr looks up an expression in the group by its index.
func (m *memoGroup) lookupExpr(eid exprID) *memoExpr {
	return &m.exprs[eid]
}

// ratchetBestExpr looks up the bestExpr that has the lowest cost for the
// given required properties. If the provided bestExpr expression has a lower
// cost, then it replaces the existing bestExpr, and ratchetBestExpr returns
// true.
func (g *memoGroup) ratchetBestExpr(required physicalPropsID, best *bestExpr) bool {
	existing := g.ensureBestExpr(required)

	// Overwrite existing best expression if the new cost is lower.
	if best.cost.Less(existing.cost) {
		*existing = *best
		return true
	}

	return false
}

// lookupBestExpr looks up the bestExpr that has the lowest cost for the given
// required properties. If no bestExpr exists yet, lookupBestExpr returns nil.
func (g *memoGroup) lookupBestExpr(required physicalPropsID) *bestExpr {
	index, ok := g.bestExprsMap[required]
	if !ok {
		return nil
	}
	return &g.bestExprs[index]
}

// ensureBestExpr looks up the bestExpr that has the lowest cost for the given
// required properties. If no bestExpr exists yet, then ensureBestExpr creates
// adds an empty bestExpr and returns it.
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
