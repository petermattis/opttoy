package opt

//go:generate optgen -out expr.og.go -pkg opt exprs ops/scalar.opt ops/relational.opt ops/enforcer.opt

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type ColMap map[ColumnIndex]ColumnIndex

// Expr is 24 bytes on a 64-bit machine, and is immutable after construction,
// so it can be passed by value. Don't reorder fields without checking its
// new size.
type Expr struct {
	mem      *memo
	loc      memoLoc
	op       Operator
	required physicalPropsID
}

func makeExpr(mem *memo, group GroupID, required physicalPropsID) Expr {
	mgrp := mem.lookupGroup(group)

	if required == defaultPhysPropsID {
		return Expr{mem: mem, loc: memoLoc{group: group, expr: normExprID}, op: mgrp.lookupExpr(normExprID).op, required: required}
	}

	best := mgrp.lookupBestExpr(required)
	return Expr{mem: mem, loc: best.loc, op: best.op, required: required}
}

func (e *Expr) Operator() Operator {
	return e.op
}

func (e *Expr) Logical() *LogicalProps {
	return e.mem.lookupGroup(e.loc.group).logical
}

// Physical returns the physical properties required of this expression, such
// as the ordering of result rows.
func (e *Expr) Physical() *PhysicalProps {
	return e.mem.lookupPhysicalProps(e.required)
}

func (e *Expr) ChildCount() int {
	return childCountLookup[e.op](e)
}

func (e *Expr) Child(nth int) Expr {
	group := e.ChildGroup(nth)
	if e.required == defaultPhysPropsID {
		return makeExpr(e.mem, group, defaultPhysPropsID)
	}

	required := e.mem.physPropsFactory.constructChildProps(e, nth)
	return makeExpr(e.mem, group, required)
}

func (e *Expr) ChildGroup(nth int) GroupID {
	return childGroupLookup[e.op](e, nth)
}

func (e *Expr) Private() interface{} {
	return e.mem.lookupPrivate(privateLookup[e.op](e))
}

func (e *Expr) String() string {
	tp := treeprinter.New()
	e.format(tp)
	return tp.String()
}

func (e *Expr) getChildGroups() []GroupID {
	children := make([]GroupID, e.ChildCount())
	for i := 0; i < e.ChildCount(); i++ {
		children[i] = e.ChildGroup(i)
	}
	return children
}

func (e *Expr) privateID() PrivateID {
	return privateLookup[e.op](e)
}

func (e *Expr) format(tp treeprinter.Node) {
	if e.IsScalar() {
		e.formatScalar(tp)
	} else {
		e.formatRelational(tp)
	}
}

func (e *Expr) formatScalar(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", e.op)
	e.formatPrivate(&buf, e.Private())

	logical := e.Logical()
	hasUnboundCols := !logical.UnboundCols.Empty()

	if hasUnboundCols {
		buf.WriteString(" [")
		if hasUnboundCols {
			fmt.Fprintf(&buf, "unbound=%s", logical.UnboundCols)
		}
		buf.WriteString("]")
	}

	tp = tp.Child(buf.String())
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		child.format(tp)
	}
}

func (e *Expr) formatPrivate(buf *bytes.Buffer, private interface{}) {
	switch e.op {
	case VariableOp:
		colIndex := private.(ColumnIndex)
		private = e.mem.metadata.ColumnLabel(colIndex)

	case ProjectionsOp:
		private = nil
	}

	if private != nil {
		fmt.Fprintf(buf, ": %v", private)
	}
}

func (e *Expr) formatRelational(tp treeprinter.Node) {
	var buf bytes.Buffer

	fmt.Fprintf(&buf, "%v", e.op)

	logicalProps := e.Logical()
	requiredProps := e.mem.lookupPhysicalProps(e.required)

	if !logicalProps.UnboundCols.Empty() {
		fmt.Fprintf(&buf, " [unbound=%s]", logicalProps.UnboundCols)
	}

	tp = tp.Child(buf.String())

	buf.Reset()

	// Write the output columns.
	if requiredProps.Projection.Defined() {
		if len(requiredProps.Projection.Columns) > 0 {
			// Write columns in required order, with required names.
			buf.WriteString("columns:")
			for _, col := range requiredProps.Projection.Columns {
				e.formatCol(&buf, col.Label, col.Index, logicalProps.Relational.NotNullCols)
			}
			tp.Child(buf.String())
		}
	} else {
		if !logicalProps.Relational.OutputCols.Empty() {
			// Fall back to writing output columns in column index order, with best
			// guess label.
			buf.WriteString("columns:")
			logicalProps.Relational.OutputCols.ForEach(func(i int) {
				e.formatCol(&buf, "", ColumnIndex(i), logicalProps.Relational.NotNullCols)
			})
			tp.Child(buf.String())
		}
	}

	// Write keys.
	for _, key := range logicalProps.Relational.WeakKeys {
		var prefix string
		if !key.SubsetOf(logicalProps.Relational.NotNullCols) {
			prefix = "weak "
		}
		tp.Childf("%skey: %s", prefix, key)
	}

	for _, fkey := range logicalProps.Relational.ForeignKeys {
		tp.Childf("foreign key: %s -> %s", fkey.src, fkey.dest)
	}

	// Write equivalent columns.
	if len(logicalProps.Relational.EquivCols) > 0 {
		buf.Reset()
		buf.WriteString("equiv:")
		for _, equiv := range logicalProps.Relational.EquivCols {
			fmt.Fprintf(&buf, " %s", equiv)
		}
		tp.Child(buf.String())
	}

	if requiredProps.Ordering.Defined() {
		tp.Childf("ordering: %s", requiredProps.Ordering.String())
	}

	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		child.format(tp)
	}
}

func (e *Expr) formatCol(buf *bytes.Buffer, label string, colIndex ColumnIndex, notNullCols ColSet) {
	metaLabel := e.mem.metadata.ColumnLabel(colIndex)
	if label == "" {
		// Use the metadata column label if there is no requested label.
		label = metaLabel
	}

	buf.WriteByte(' ')
	buf.WriteString(label)
	buf.WriteByte(':')
	fmt.Fprintf(buf, "%d", colIndex)
	if notNullCols.Contains(int(colIndex)) {
		buf.WriteByte('*')
	}
}
