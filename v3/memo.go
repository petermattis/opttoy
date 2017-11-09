package v3

import (
	"bytes"
	"fmt"
)

// TODO(peter):
// - Extract expressions from the memo for transformation

type memoExpr struct {
	class    int32
	op       operator
	auxBits  uint16
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

	if e.auxBits != 0 {
		buf.WriteString(" ")
		if (e.auxBits & (1 << auxApplyBit)) != 0 {
			buf.WriteString("a")
		}
		if (e.auxBits & (1 << auxFilterBit)) != 0 {
			buf.WriteString("f")
		}
		if (e.auxBits & (1 << aux1Bit)) != 0 {
			buf.WriteString("1")
		}
		if (e.auxBits & (1 << aux2Bit)) != 0 {
			buf.WriteString("2")
		}
	}

	if len(e.children) > 0 {
		fmt.Fprintf(&buf, " [")
		for i, c := range e.children {
			if i > 0 {
				buf.WriteString(" ")
			}
			fmt.Fprintf(&buf, "%d", c)
		}
		fmt.Fprintf(&buf, "]")
	}
	return buf.String()
}

type memoClass struct {
	// A map from memo expression fingerprint to the index of the memo expression
	// in the exprs slice. Used to determine if a memoExpr already exists in the
	// class.
	exprMap map[string]int32
	exprs   []*memoExpr
	// The logical properties for the class. The logical properties are nil for
	// scalar expressions.
	props *logicalProps
	// Scalar expressions only have a single memoExpr per class. So we cache the
	// full expression tree to make extraction faster.
	scalarExpr *expr
}

func newMemoClass(props *logicalProps) *memoClass {
	return &memoClass{
		exprMap: make(map[string]int32),
		props:   props,
	}
}

func (c *memoClass) maybeAddExpr(e *memoExpr) {
	f := e.fingerprint()
	if _, ok := c.exprMap[f]; !ok {
		c.exprMap[f] = int32(len(c.exprs))
		c.exprs = append(c.exprs, e)
	}
}

type memo struct {
	// A map from class fingerprint to the index of the class in the classes
	// slice. For relational classes, the fingerprint for a class is the
	// fingerprint of the logical properties. For scalar classes, the fingerprint
	// for a class is the fingerprint of the memo expression.
	classMap map[string]int32
	classes  []*memoClass
}

func newMemo() *memo {
	return &memo{
		classMap: make(map[string]int32),
	}
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for _, id := range m.topologicalSort() {
		fmt.Fprintf(&buf, "%d:", id)
		c := m.classes[id]
		for _, e := range c.exprs {
			fmt.Fprintf(&buf, " [%s]", e.fingerprint())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

func (m *memo) topologicalSort() []int32 {
	visited := make([]bool, len(m.classes))
	res := make([]int32, 0, len(m.classes))
	for id := range m.classes {
		res = m.dfsVisit(int32(id), visited, res)
	}

	// The depth first search returned the classes from leaf to root. We want the
	// root first, so reverse the results.
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

func (m *memo) dfsVisit(id int32, visited []bool, res []int32) []int32 {
	if visited[id] {
		return res
	}
	visited[id] = true

	c := m.classes[id]
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
		auxBits:  e.auxBits,
		children: make([]int32, len(e.children)),
		private:  e.private,
	}
	for i, c := range e.children {
		me.children[i] = m.addExpr(c)
	}

	if e.props != nil {
		// We have a relational expression. Find the class the memoExpr would exist
		// in.
		me.class = m.maybeAddClass(e.props.fingerprint(), e.props)
		c := m.classes[me.class]
		c.maybeAddExpr(me)
	} else {
		// We have a scalar expression. Use the expression fingerprint as the class
		// fingerprint.
		me.class = m.maybeAddClass(me.fingerprint(), nil)
		c := m.classes[me.class]
		if c.scalarExpr == nil {
			c.scalarExpr = e
		}
		c.maybeAddExpr(me)
	}

	return me.class
}

func (m *memo) maybeAddClass(f string, props *logicalProps) int32 {
	id, ok := m.classMap[f]
	if !ok {
		id = int32(len(m.classes))
		c := newMemoClass(props)
		m.classes = append(m.classes, c)
		m.classMap[f] = id
	}
	return id
}
