package v3

import (
	"bytes"
	"fmt"
)

// TODO(peter):
// - Extract expressions from the memo for transformation

type memoExpr struct {
	group    int32
	op       operator
	extra    uint8
	apply    bool
	children []int32
	private  interface{}
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
	// Scalar expressions only have a single memoExpr per group. So we cache the
	// full expression tree to make extraction faster.
	//
	// TODO(peter): Does this work when a scalar expression can contain a
	// relational expression?
	scalarExpr *expr
}

func newMemoGroup(props *logicalProps) *memoGroup {
	return &memoGroup{
		exprMap: make(map[string]int32),
		props:   props,
	}
}

func (c *memoGroup) maybeAddExpr(e *memoExpr) {
	f := e.fingerprint()
	if _, ok := c.exprMap[f]; !ok {
		c.exprMap[f] = int32(len(c.exprs))
		c.exprs = append(c.exprs, e)
	}
}

type memo struct {
	// A map from group fingerprint to the index of the group in the groups
	// slice. For relational groups, the fingerprint for a group is the
	// fingerprint of the logical properties. For scalar groups, the fingerprint
	// for a group is the fingerprint of the memo expression.
	groupMap map[string]int32
	groups   []*memoGroup
}

func newMemo() *memo {
	return &memo{
		groupMap: make(map[string]int32),
	}
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for _, id := range m.topologicalSort() {
		fmt.Fprintf(&buf, "%d:", id)
		c := m.groups[id]
		for _, e := range c.exprs {
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

	c := m.groups[id]
	for _, e := range c.exprs {
		for _, v := range e.children {
			res = m.dfsVisit(v, visited, res)
		}
	}
	return append(res, id)
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
	for i, c := range e.children {
		if c != nil {
			me.children[i] = m.addExpr(c)
		} else {
			me.children[i] = -1
		}
	}

	if e.props != nil {
		// We have a relational expression. Find the group the memoExpr would exist
		// in.
		me.group = m.maybeAddGroup(e.props.fingerprint(), e.props)
		c := m.groups[me.group]
		c.maybeAddExpr(me)
	} else {
		// We have a scalar expression. Use the expression fingerprint as the group
		// fingerprint.
		me.group = m.maybeAddGroup(me.fingerprint(), nil)
		c := m.groups[me.group]
		if c.scalarExpr == nil {
			c.scalarExpr = e
		}
		c.maybeAddExpr(me)
	}

	return me.group
}

func (m *memo) maybeAddGroup(f string, props *logicalProps) int32 {
	id, ok := m.groupMap[f]
	if !ok {
		id = int32(len(m.groups))
		c := newMemoGroup(props)
		m.groups = append(m.groups, c)
		m.groupMap[f] = id
	}
	return id
}

func (m *memo) newBinder(group, idx int32) *memoBinder {
	c := m.groups[group]
	return &memoBinder{
		memo:  m,
		group: c,
		mexpr: c.exprs[idx],
	}
}

type memoBinder struct {
	memo    *memo
	group   *memoGroup
	mexpr   *memoExpr
	pattern *expr
}

// Specify the pattern to bind with. The binder will be reset and bound
// expressions can be iterated over using next().
func (b *memoBinder) bind(pattern *expr) {
	if pattern.op != b.mexpr.op {
		fatalf("pattern root does not match expression root: %s != %s", pattern.op, b.mexpr.op)
	}
	b.pattern = pattern
}

func (b *memoBinder) next() *expr {
	return nil
}
