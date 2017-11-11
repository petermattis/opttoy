package v3

import (
	"bytes"
	"fmt"
	"sync"
)

// TODO(peter):
// - Extract expressions from the memo for transformation

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
	extra    uint8
	apply    bool
	children []int32
	private  interface{}
}

func (e *memoExpr) match(pattern *expr) bool {
	return isPatternOp(pattern) || pattern.op == e.op
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

	if e.apply {
		buf.WriteString(" (apply)")
	}

	if len(e.children) > 0 {
		fmt.Fprintf(&buf, " [")
		for i, c := range e.children {
			if i > 0 {
				buf.WriteString(" ")
			}
			if c < 0 {
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
	// A map from memo expression fingerprint to the index of the memo expression
	// in the exprs slice. Used to determine if a memoExpr already exists in the
	// group.
	exprMap map[string]int32
	exprs   []*memoExpr
	// The logical properties for the group. The logical properties are nil for
	// scalar expressions.
	props *logicalProps

	// TODO(peter): Cache scalar expressions that do not contain subqueries.
}

func newMemoGroup(props *logicalProps) *memoGroup {
	return &memoGroup{
		exprMap: make(map[string]int32),
		props:   props,
	}
}

func (g *memoGroup) maybeAddExpr(e *memoExpr) {
	f := e.fingerprint()
	if _, ok := g.exprMap[f]; !ok {
		e.loc.expr = int32(len(g.exprs))
		g.exprMap[f] = e.loc.expr
		g.exprs = append(g.exprs, e)
	}
}

type memo struct {
	// A map from group fingerprint to the index of the group in the groups
	// slice. For relational groups, the fingerprint for a group is the
	// fingerprint of the logical properties. For scalar groups, the fingerprint
	// for a group is the fingerprint of the memo expression.
	groupMap map[string]int32
	groups   []*memoGroup
	root     int32
}

func newMemo() *memo {
	return &memo{
		groupMap: make(map[string]int32),
		root:     -1,
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
	if id < 0 || visited[id] {
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
	if m.root != -1 {
		fatalf("root has already been set")
	}
	m.root = m.addExpr(e)
}

func (m *memo) addExpr(e *expr) int32 {
	// Build a memoExpr and check to see if it already exists in the memo.
	me := &memoExpr{
		op:       e.op,
		extra:    e.extra,
		apply:    e.apply,
		children: make([]int32, len(e.children)),
		private:  e.private,
	}
	for i, g := range e.children {
		if g != nil {
			me.children[i] = m.addExpr(g)
		} else {
			me.children[i] = -1
		}
	}

	if e.props != nil {
		// We have a relational expression. Find the group the memoExpr would exist
		// in.
		me.loc.group = m.maybeAddGroup(e.props.fingerprint(), e.props)
	} else {
		// We have a scalar expression. Use the expression fingerprint as the group
		// fingerprint.
		me.loc.group = m.maybeAddGroup(me.fingerprint(), nil)
	}

	g := m.groups[me.loc.group]
	g.maybeAddExpr(me)
	return me.loc.group
}

func (m *memo) maybeAddGroup(f string, props *logicalProps) int32 {
	id, ok := m.groupMap[f]
	if !ok {
		id = int32(len(m.groups))
		m.groups = append(m.groups, newMemoGroup(props))
		m.groupMap[f] = id
	}
	return id
}

// Bind creates a cursor expression rooted at the specified location. The
// pattern specifies the structure of the cursor. Returns nil if the pattern
// does not match an expression rooted at the specified location. The cursor
// can be iterated by passing the result from a previous call to bind() as the
// cursor argument.
//
// Note that the returned expression is only valid until the next call to
// bind().
func (m *memo) bind(loc memoLoc, pattern *expr, cursor *expr) *expr {
	g := m.groups[loc.group]
	e := g.exprs[loc.expr]
	if e.op != pattern.op {
		return nil
	}
	return m.bindExpr(e, pattern, cursor)
}

func (m *memo) bindExpr(e *memoExpr, pattern, cursor *expr) *expr {
	if !e.match(pattern) {
		return nil
	}

	if cursor != nil && e.loc != cursor.loc {
		fatalf("invalid bind expr: %s != %s", e.loc, cursor.loc)
	}

	g := m.groups[e.loc.group]
	var initChildren bool
	if cursor == nil {
		cursor = newMemoCursor()
		cursor.props = g.props
		initChildren = true
	}
	cursor.op = e.op
	cursor.extra = e.extra
	cursor.apply = e.apply
	cursor.loc = e.loc
	cursor.private = e.private

	if len(cursor.children) != len(e.children) {
		if cap(cursor.children) >= len(e.children) {
			cursor.children = cursor.children[:len(e.children)]
			for i := range cursor.children {
				cursor.children[i] = nil
			}
		} else {
			cursor.children = make([]*expr, len(e.children))
		}
	}

	if isPatternLeaf(pattern) {
		return cursor
	}

	if initChildren {
		// Initialize the child cursors.
		for i, g := range e.children {
			childPattern := pattern
			if !isPatternTree(pattern) {
				childPattern = pattern.children[i]
			}

			if g != -1 {
				cursor.children[i] = m.bindGroup(m.groups[g], childPattern, nil)
				if cursor.children[i] == nil {
					// Pattern failed to match.
					return nil
				}
			} else if !isPatternOp(pattern) {
				// No child present and pattern failed to match.
				return nil
			}
		}
		return cursor
	}

	var valid int
	for _, c := range cursor.children {
		if c != nil {
			valid++
		}
	}

	// Advance the child cursors.
	var exhausted int
	for i, n := 0, len(cursor.children); i < n; i++ {
		childCursor := cursor.children[i]
		if childCursor == nil {
			continue
		}

		childPattern := pattern
		if !isPatternTree(pattern) {
			childPattern = pattern.children[i]
		}

		g := m.groups[childCursor.loc.group]
		childCursor = m.bindGroup(g, childPattern, childCursor)
		if childCursor != nil {
			// We successfully advanced a child.
			cursor.children[i] = childCursor
			return cursor
		}

		releaseMemoCursor(childCursor)

		exhausted++
		if exhausted >= valid {
			// We exhausted all of the child cursors. Nothing more for us to do.
			return nil
		}

		// Reset the child cursor.
		cursor.children[i] = m.bindGroup(g, childPattern, nil)
	}
	return cursor
}

func (m *memo) bindGroup(g *memoGroup, pattern, cursor *expr) *expr {
	exprs := g.exprs
	if cursor != nil {
		exprs = g.exprs[cursor.loc.expr:]
	}

	if isPatternLeaf(pattern) {
		// For leaf patterns we do not iterate on groups.
		if cursor != nil {
			// If a leaf was extracted before, we've exhaused the group.
			releaseMemoCursor(cursor)
			return nil
		}
		return m.bindExpr(g.exprs[0], pattern, cursor)
	}

	for _, e := range exprs {
		if !e.match(pattern) {
			continue
		}
		last := cursor
		cursor = m.bindExpr(e, pattern, cursor)
		if cursor != nil {
			return cursor
		}
		releaseMemoCursor(last)
	}

	// We've exhausted the group.
	return nil
}

var cursorPool = sync.Pool{
	New: func() interface{} {
		return &expr{}
	},
}

func newMemoCursor() *expr {
	return cursorPool.Get().(*expr)
}

func releaseMemoCursor(cursor *expr) {
	if cursor != nil {
		for _, child := range cursor.children {
			releaseMemoCursor(child)
		}
		*cursor = expr{}
		cursorPool.Put(cursor)
	}
}
