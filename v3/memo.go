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
	id    int32
	m     map[string]*memoExpr
	exprs []*memoExpr
	props *logicalProps
}

func newMemoClass(id int32, props *logicalProps) *memoClass {
	return &memoClass{
		id:    id,
		m:     make(map[string]*memoExpr),
		props: props,
	}
}

func (c *memoClass) maybeAddExpr(e *memoExpr) {
	f := e.fingerprint()
	if _, ok := c.m[f]; !ok {
		c.exprs = append(c.exprs, e)
		c.m[f] = e
	}
}

type memo struct {
	classMap map[string]int32
	classes  []*memoClass
}

func newMemo() *memo {
	return &memo{
		classMap: make(map[string]int32),
	}
}

func (m *memo) String() string {
	// TODO(peter): topological sort.
	var buf bytes.Buffer
	for _, c := range m.classes {
		fmt.Fprintf(&buf, "%d:", c.id)
		for _, e := range c.exprs {
			fmt.Fprintf(&buf, " [%s]", e.fingerprint())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
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
	} else {
		// We have a scalar expression. Use the expression fingerprint as the class
		// fingerprint.
		me.class = m.maybeAddClass(me.fingerprint(), e.props)
	}

	c := m.classes[me.class]
	c.maybeAddExpr(me)
	return me.class
}

func (m *memo) maybeAddClass(f string, props *logicalProps) int32 {
	id, ok := m.classMap[f]
	if !ok {
		id = int32(len(m.classes))
		c := newMemoClass(id, props)
		m.classes = append(m.classes, c)
		m.classMap[f] = id
	}
	return id
}
