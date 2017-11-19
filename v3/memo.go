package v3

import (
	"bytes"
	"fmt"
	"sort"
)

// groupID identifies a memo group. Groups have numbers greater than 0; a
// groupID of 0 indicates an empty expression or an unknown group.
type groupID int32

// exprID identifies an expression within its memo group.
type exprID int32

// memoLoc describes the location of an expression in the memo.
type memoLoc struct {
	group groupID
	expr  exprID
}

func (l memoLoc) String() string {
	return fmt.Sprintf("%d.%d", l.group, l.expr)
}

// memoExpr is a memoized representation of an expression. Unlike expr which
// represents a single expression, a memoExpr roots a forest of
// expressions. This is accomplished by recursively memoizing children and
// storing logically equivalent expressions in the memo
// structure. memoExpr.children refers to child groups of logically equivalent
// expressions. Because memoExpr refers to a forest of expressions, it is
// challenging to perform transformations directly upon it. Instead,
// transformations are performed by extracting an expr fragment matching a
// pattern from the memo, performing the transformation and then inserting the
// transformed result back into the memo.
//
// For relational expressions, logical equivalency is defined as equivalent
// relational properties (see relationalProps.fingerprint()). For scalar
// expressions, logical equivalency is defined as equivalent memoExpr (see
// memoExpr.fingerprint()). While scalar expressions are stored in the memo,
// each scalar expression group contains only a single entry.
type memoExpr struct {
	op            operator       // expr.op
	loc           memoLoc        // expr.loc
	children      []groupID      // expr.children
	physicalProps *physicalProps // expr.physicalProps
	private       interface{}    // expr.private
	// NB: expr.{props,scalarProps} are the same for all expressions in the group
	// and stored in memoGroup.
}

func (e *memoExpr) matchOp(pattern *expr) bool {
	return isPatternSentinel(pattern) || pattern.op == e.op
}

// fingerprint returns a string which uniquely identifies the expression within
// the context of the memo.
func (e *memoExpr) fingerprint() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", e.op)

	switch t := e.private.(type) {
	case nil:
	case *table:
		fmt.Fprintf(&buf, " %s", t.name)
	default:
		fmt.Fprintf(&buf, " %s", e.private)
	}

	if f := e.physicalProps.fingerprint(); f != "" {
		fmt.Fprintf(&buf, " %s", f)
	}

	if len(e.children) > 0 {
		fmt.Fprintf(&buf, " [")
		for i, c := range e.children {
			if i > 0 {
				buf.WriteString(" ")
			}
			if c <= 0 {
				buf.WriteString("-")
			} else {
				fmt.Fprintf(&buf, "%d", c)
			}
		}
		fmt.Fprintf(&buf, "]")
	}
	return buf.String()
}

// memoGroup stores a set of logically equivalent expressions. See the comments
// on memoExpr for the definition of logical equivalency.
type memoGroup struct {
	// The ID (a.k.a. index, memoLoc.group) of the group within the memo.
	id groupID
	// The index of the last explored expression. Used by search.
	explored exprID
	// The index of the last implemented expression. Used by search.
	implemented exprID
	// The index of the last optimized expression. Used by search.
	optimized exprID

	// A map from memo expression fingerprint to the index of the memo expression
	// in the exprs slice. Used to determine if a memoExpr already exists in the
	// group.
	exprMap map[string]exprID
	exprs   []*memoExpr
	// The relational properties for the group. Nil if the group contains scalar
	// expressions.
	props *relationalProps
	// The scalar properties for the group. Nil if the group contains relational
	// expressions.
	scalarProps *scalarProps
}

func newMemoGroup(props *relationalProps, scalarProps *scalarProps) *memoGroup {
	return &memoGroup{
		exprMap:     make(map[string]exprID),
		props:       props,
		scalarProps: scalarProps,
	}
}

type memo struct {
	// A map from expression fingerprint (memoExpr.fingerprint()) to the index of
	// the group the expression resides in.
	exprMap map[string]groupID
	// A map from group fingerprint to the index of the group in the groups
	// slice. For relational groups, the fingerprint for a group is the
	// fingerprint of the relational properties
	// (relationalProps.fingerprint()). For scalar groups, the fingerprint for a
	// group is the fingerprint of the memo expression (memoExpr.fingerprint()).
	groupMap map[string]groupID
	// The slice of groups, indexed by group ID (i.e. memoLoc.group). Note the
	// group ID 0 is invalid in order to allow zero initialization of expr to
	// indicate an expression that did not originate from the memo.
	groups []*memoGroup
	// The root group in the memo. This is the group for the expression added by
	// addRoot (i.e. the expression that we're optimizing).
	root groupID
}

func newMemo() *memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression.
	return &memo{
		exprMap:  make(map[string]groupID),
		groupMap: make(map[string]groupID),
		groups:   make([]*memoGroup, 1),
	}
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for _, id := range m.topologicalSort() {
		g := m.groups[id]
		// TODO(peter): provide a better mechanism for displaying group
		// fingerprints for debugging.
		if false && g.props != nil {
			fmt.Fprintf(&buf, "%d [%s]:", id, g.props.fingerprint())
		} else {
			fmt.Fprintf(&buf, "%d:", id)
		}
		for _, e := range g.exprs {
			fmt.Fprintf(&buf, " [%s]", e.fingerprint())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

// topologicalSort returns an ordering of memo groups such that if an expression
// in group i points to group j, i comes before j in the ordering.
func (m *memo) topologicalSort() []groupID {
	visited := make([]bool, len(m.groups))
	res := make([]groupID, 0, len(m.groups))
	for id := range m.groups[1:] {
		res = m.dfsVisit(groupID(id+1), visited, res)
	}

	// The depth first search returned the groups from leaf to root. We want the
	// root first, so reverse the results.
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

// dfsVisit performs a depth-first search starting from the group, avoiding
// already visited nodes. Returns the visited node in depth-first order.
func (m *memo) dfsVisit(id groupID, visited []bool, res []groupID) []groupID {
	if id <= 0 || visited[id] {
		return res
	}
	visited[id] = true

	g := m.groups[id]
	for _, e := range g.exprs {
		for _, v := range e.children {
			res = m.dfsVisit(v, visited, res)
		}
	}
	return append(res, id)
}

func (m *memo) addRoot(e *expr) {
	if m.root != 0 {
		fatalf("root has already been set")
	}
	m.root = m.addExpr(e)
}

// Check performs consistency checks of the memo internal data structures.
func (m *memo) check() {
	for i, g := range m.groups {
		if g == nil {
			continue
		}
		var gf string
		if g.props != nil {
			gf = g.props.fingerprint()
		} else {
			gf = g.exprs[0].fingerprint()
		}
		group, ok := m.groupMap[gf]
		if !ok {
			fatalf("group %d not found: %s", i, gf)
		}
		if group != groupID(i) {
			fatalf("expected group %d, but found %d: %s", i, group, gf)
		}
	}

	for gf, group := range m.groupMap {
		g := m.groups[group]
		if g.props != nil {
			tf := g.props.fingerprint()
			if gf != tf {
				fatalf("relational group %d, unexpected fingerprint: %s != %s", group, gf, tf)
			}
		} else {
			ef := g.exprs[0].fingerprint()
			if gf != ef {
				fatalf("scalar group %d, unexpected fingerprint: %s != %s", group, gf, ef)
			}
		}
	}
}

// addExpr adds an expression to the memo and returns the group it was added to.
func (m *memo) addExpr(e *expr) groupID {
	m.check()

	if e.loc.group > 0 && e.loc.expr >= 0 {
		// The expression has already been added to the memo.
		return e.loc.group
	}

	// Build a memoExpr and check to see if it already exists in the memo.
	me := &memoExpr{
		op:            e.op,
		loc:           e.loc,
		children:      make([]groupID, len(e.children)),
		physicalProps: e.physicalProps,
		private:       e.private,
	}
	for i, g := range e.children {
		if g != nil {
			me.children[i] = m.addExpr(g)
		}
	}

	// TODO(peter): Figure out a way to remove this hack. We normalize the list
	// op expressions by sorting them by group.
	if me.op == listOp {
		sort.Slice(me.children, func(i, j int) bool {
			return me.children[i] < me.children[j]
		})
	}

	ef := me.fingerprint()
	if me.loc.group == 0 {
		if group, ok := m.exprMap[ef]; ok {
			// Expression already exists in the memo.
			if e.props != nil {
				// Check that the logical properties map to the group the expression
				// already exists in.
				if newGroup := m.groupMap[e.props.fingerprint()]; group != newGroup {
					fatalf("group mismatch for existing expression\n%d: [%s]\n%d: [%s]\n%s",
						group, m.groups[group].props.fingerprint(),
						newGroup, e.props.fingerprint(),
						e)
				}
			}
			return group
		}

		// Determine which group the expression belongs in, creating it if
		// necessary.
		gf := ef
		if e.props != nil {
			gf = e.props.fingerprint()
		}
		group, ok := m.groupMap[gf]
		if !ok {
			group = groupID(len(m.groups))
			g := newMemoGroup(e.props, e.scalarProps)
			g.id = group
			m.groups = append(m.groups, g)
			m.groupMap[gf] = group
		}
		me.loc.group = group
	}

	g := m.groups[me.loc.group]
	if _, ok := g.exprMap[ef]; !ok {
		me.loc.expr = exprID(len(g.exprs))
		g.exprMap[ef] = me.loc.expr
		g.exprs = append(g.exprs, me)
	}
	m.exprMap[ef] = me.loc.group
	return me.loc.group
}

// Bind creates a cursor expression rooted at the specified location that
// matches the pattern. The cursor can be advanced with calls to advance().
//
// Returns nil if the pattern does not match any expression rooted at the
// specified location.
func (m *memo) bind(e *memoExpr, pattern *expr) *expr {
	if !e.matchOp(pattern) {
		return nil
	}

	g := m.groups[e.loc.group]
	cursor := &expr{
		props:       g.props,
		scalarProps: g.scalarProps,
		op:          e.op,
		loc:         e.loc,
		children:    make([]*expr, len(e.children)),
		private:     e.private,
	}

	if isPatternLeaf(pattern) {
		return cursor
	}

	// Initialize the child cursors.
	for i, g := range e.children {
		childPattern := childPattern(pattern, i)
		if g == 0 {
			// No child present.
			if !isPatternSentinel(childPattern) {
				return nil
			}
			// Leave the nil cursor, it will be skipped by advance.
			continue
		}

		cursor.children[i] = m.bindGroup(m.groups[g], childPattern)
		if cursor.children[i] == nil {
			// Pattern failed to match.
			return nil
		}
	}
	return cursor
}

// advance returns the next cursor expression that matches the pattern.
// The cursor must have been obtained from a previous call to bind() or
// advance().
//
// Returns nil if there are no more expressions that match.
func (m *memo) advance(e *memoExpr, pattern, cursor *expr) *expr {
	if e.loc != cursor.loc || !e.matchOp(pattern) {
		fatalf("cursor mismatch: e: %s %s  cursor: %s %s", e.op, e.loc, cursor.op, cursor.loc)
	}

	if isPatternLeaf(pattern) {
		// For a leaf pattern we have only the initial binding.
		return nil
	}

	// We first advance the first child cursor; when that is exhausted, we reset
	// it and advance the second cursor. Next time we will start over with
	// advancing the first child cursor until it is exhausted.
	//
	// For example, say we have three children with 2 bindings each:
	//            child 0  child 1  child 2
	// bind:      0        0        0
	// advance:   1        0        0
	// advance:   0        1        0
	// advance:   1        1        0
	// advance:   0        0        1
	// advance:   1        0        1
	// advance:   0        1        1
	// advance:   1        1        1
	// advance:   done
	//
	// This is somewhat analogous to incrementing an integer (children are digits,
	// in reverse order).
	for i, childCursor := range cursor.children {
		if childCursor == nil {
			// Skip the missing child (it must be a pattern leaf).
			continue
		}

		childPattern := childPattern(pattern, i)

		g := m.groups[childCursor.loc.group]
		cursor.children[i] = m.advanceGroup(g, childPattern, childCursor)
		if cursor.children[i] != nil {
			// We successfully advanced a child.
			return cursor
		}

		// Reset the child cursor and advance to the next child.
		cursor.children[i] = m.bindGroup(g, childPattern)
	}
	// We exhausted all child cursors. Nothing more for us to do.
	return nil
}

// bindGroup is similar to bind, except that it can bind any expression
// rooted in the given group.
//
// Returns a cursor expression that can be advanced with advanceGroup().
//
// Returns nil if the pattern does not match any expression rooted at the
// specified location.
func (m *memo) bindGroup(g *memoGroup, pattern *expr) *expr {
	for _, e := range g.exprs {
		if !e.matchOp(pattern) {
			continue
		}
		if cursor := m.bind(e, pattern); cursor != nil {
			return cursor
		}
	}

	// The group has no expressions that match the pattern.
	return nil
}

// advanceGroup advances a cursor expression obtained from a previous call to
// bindGroup() or advanceGroup().
//
// Returns nil if there are no more expressions in the group that match the
// pattern.
func (m *memo) advanceGroup(g *memoGroup, pattern, cursor *expr) *expr {
	if isPatternLeaf(pattern) {
		// For leaf patterns we do not iterate on groups.
		return nil
	}

	// Try to advance the binding for the current expression.
	if c := m.advance(g.exprs[cursor.loc.expr], pattern, cursor); c != nil {
		return c
	}

	// Find another expression to bind.
	for _, e := range g.exprs[(cursor.loc.expr + 1):] {
		if !e.matchOp(pattern) {
			continue
		}
		if c := m.bind(e, pattern); c != nil {
			return c
		}
	}

	// We've exhausted the group.
	return nil
}
