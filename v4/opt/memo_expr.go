package opt

import (
	"bytes"
	"fmt"
)

// memoExpr is a memoized representation of an expression. Specializations of
// memoExpr are generated by optgen for each operator (see Expr.og.go). Each
// memoExpr belongs to a memo group, which contain logically equivalent
// expressions. Two expressions are considered logically equivalent if they
// both reduce to an identical normal form after normalizing transformations
// have been applied.
//
// The children of memoExpr are recursively memoized in the same way as the
// memoExpr, and are referenced by their memo group. Therefore, the memoExpr
// is the root of a forest of expressions. Each memoExpr is memoized by its
// fingerprint, which is the hash of its op type plus the group ids of its
// children.
//
// Don't change the order of the fields in memoExpr. The op field is second in
// order to make the generated fingerprint methods faster and easier to
// implement.
type memoExpr struct {
	// group identifies the memo group to which this expression belongs.
	group GroupID

	// op is this expression's operator type. Each operator may have additional
	// fields. To access these fields, use the asXXX() generated methods to
	// cast the memoExpr to the more specialized expression type.
	op Operator
}

func (me *memoExpr) MemoString(mem *memo) string {
	var buf bytes.Buffer

	e := makeExpr(mem, me.group, defaultPhysPropsID)

	fmt.Fprintf(&buf, "[%s", e.Operator())

	private := e.Private()
	if private != nil {
		switch t := private.(type) {
		case nil:
		case TableIndex:
			fmt.Fprintf(&buf, " %s", mem.metadata.Table(t).Table.Name)
		case ColumnIndex:
			fmt.Fprintf(&buf, " %s", mem.metadata.ColumnLabel(t))
		case *ColSet, *ColMap:
			// Don't show anything, because it's mostly redundant.
		default:
			fmt.Fprintf(&buf, " %s", private)
		}
	}

	if e.ChildCount() > 0 {
		fmt.Fprintf(&buf, " [")
		for i := 0; i < e.ChildCount(); i++ {
			child := e.ChildGroup(i)
			if i > 0 {
				buf.WriteString(" ")
			}
			if child <= 0 {
				buf.WriteString("-")
			} else {
				fmt.Fprintf(&buf, "%d", child)
			}
		}
		fmt.Fprintf(&buf, "]")
	}

	buf.WriteString("]")
	return buf.String()
}