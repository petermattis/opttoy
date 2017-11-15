package v3

import (
	"bytes"
	"fmt"
)

type memoLoc struct {
	group int32
	expr  int32
}

func (l memoLoc) String() string {
	return fmt.Sprintf("%d.%d", l.group, l.expr)
}

type memoExpr struct {
	loc      memoLoc
	op       operator
	children []int32
	private  interface{}
}

func (e *memoExpr) match(pattern *expr) bool {
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

type memoGroup struct {
	id          int32
	explored    int32
	implemented int32

	// A map from memo expression fingerprint to the index of the memo expression
	// in the exprs slice. Used to determine if a memoExpr already exists in the
	// group.
	exprMap map[string]int32
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
		exprMap:     make(map[string]int32),
		props:       props,
		scalarProps: scalarProps,
	}
}

type memo struct {
	// A map from expression fingerprint to the index of the group the expression
	// resides in.
	exprMap map[string]int32
	// A map from group fingerprint to the index of the group in the groups
	// slice. For relational groups, the fingerprint for a group is the
	// fingerprint of the relational properties. For scalar groups, the
	// fingerprint for a group is the fingerprint of the memo expression.
	groupMap map[string]int32
	groups   []*memoGroup
	root     int32
}

func newMemo() *memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression.
	return &memo{
		exprMap:  make(map[string]int32),
		groupMap: make(map[string]int32),
		groups:   make([]*memoGroup, 1),
	}
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for _, id := range m.topologicalSort() {
		fmt.Fprintf(&buf, "%d:", id)
		g := m.groups[id]
		for _, e := range g.exprs {
			fmt.Fprintf(&buf, " [%s]", e.fingerprint())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

func (m *memo) topologicalSort() []int32 {
	visited := make([]bool, len(m.groups))
	res := make([]int32, 0, len(m.groups))
	for id := range m.groups {
		res = m.dfsVisit(int32(id), visited, res)
	}

	// The depth first search returned the groups from leaf to root. We want the
	// root first, so reverse the results.
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

func (m *memo) dfsVisit(id int32, visited []bool, res []int32) []int32 {
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

func (m *memo) addExpr(e *expr) int32 {
	if e.loc.group > 0 && e.loc.expr >= 0 {
		// The expression has already been added to the memo.
		return e.loc.group
	}

	// Build a memoExpr and check to see if it already exists in the memo.
	me := &memoExpr{
		op:       e.op,
		loc:      e.loc,
		children: make([]int32, len(e.children)),
		private:  e.private,
	}
	for i, g := range e.children {
		if g != nil {
			me.children[i] = m.addExpr(g)
		}
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
		pf := ef
		if e.props != nil {
			pf = e.props.fingerprint()
		}
		group, ok := m.groupMap[pf]
		if !ok {
			group = int32(len(m.groups))
			g := newMemoGroup(e.props, e.scalarProps)
			g.id = group
			m.groups = append(m.groups, g)
			m.groupMap[pf] = group
		}
		me.loc.group = group
	}

	g := m.groups[me.loc.group]
	if _, ok := g.exprMap[ef]; !ok {
		me.loc.expr = int32(len(g.exprs))
		g.exprMap[ef] = me.loc.expr
		g.exprs = append(g.exprs, me)
	}
	m.exprMap[ef] = me.loc.group
	return me.loc.group
}

func childPattern(pattern *expr, childIdx int) *expr {
	if isPatternTree(pattern) {
		return patternTree
	}
	return pattern.children[childIdx]
}

// Bind creates a cursor expression rooted at the specified location that
// matches the pattern. The cursor can be advanced with calls to advance().
//
// Returns nil if the pattern does not match any expression rooted at the
// specified location.
func (m *memo) bind(e *memoExpr, pattern *expr) *expr {
	if !e.match(pattern) {
		return nil
	}

	g := m.groups[e.loc.group]
	cursor := &expr{
		props:       g.props,
		scalarProps: g.scalarProps,
		op:          e.op,
		loc:         e.loc,
		private:     e.private,
	}

	if isPatternLeaf(pattern) {
		return cursor
	}

	cursor.children = make([]*expr, len(e.children))

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
// Returns nil if there are no more expressions that match.
func (m *memo) advance(e *memoExpr, pattern, cursor *expr) *expr {
	if e.loc != cursor.loc {
		fatalf("invalid bind expr: %s != %s", e.loc, cursor.loc)
	}

	if !e.match(pattern) {
		fatalf("non-matching bind expr")
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

// bindGroup is similar to bind, except that it can bind any expression routed
// in the given group.
//
// Returns a cursor expression that can be advanced with advanceGroup().
//
// Returns nil if the pattern does not match any expression rooted at the
// specified location.
func (m *memo) bindGroup(g *memoGroup, pattern *expr) *expr {
	for _, e := range g.exprs {
		if !e.match(pattern) {
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
		if !e.match(pattern) {
			continue
		}
		if c := m.bind(e, pattern); c != nil {
			return c
		}
	}

	// We've exhausted the group.
	return nil
}
