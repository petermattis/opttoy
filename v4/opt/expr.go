package opt

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

//go:generate optgen -out expr.og.go -pkg opt exprs ops/scalar.opt ops/relational.opt ops/enforcer.opt

// Expr is 24 bytes on a 64-bit machine, and is immutable after construction,
// so it can be passed by value.
type Expr struct {
	mem      *memo
	group    GroupID
	op       Operator
	offset   exprOffset
	required physicalPropsID
}

func (e *Expr) Operator() Operator {
	return e.op
}

func (e *Expr) Logical() *LogicalProps {
	return e.mem.lookupGroup(e.group).logical
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
	required := e.mem.physPropsFactory.constructRequiredProps(e, nth)
	best := e.mem.lookupGroup(group).lookupBestExpr(required)
	return Expr{mem: e.mem, group: group, op: best.op, offset: best.offset, required: required}
}

func (e *Expr) ChildGroup(nth int) GroupID {
	return childGroupLookup[e.op](e, nth)
}

func (e *Expr) Private() interface{} {
	return e.mem.lookupPrivate(privateIDLookup[e.op](e))
}

func (e *Expr) String() string {
	tp := treeprinter.New()
	e.format(tp)
	return tp.String()
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

	private := e.Private()
	if private != nil {
		fmt.Fprintf(&buf, " (%s)", private)
	}

	logical := e.Logical()
	buf.WriteString(" [")
	if !logical.UnboundCols.Empty() {
		fmt.Fprintf(&buf, "in=%s", logical.UnboundCols)
	}
	buf.WriteString("]")

	tp = tp.Child(buf.String())
	for i := 0; i < e.ChildCount(); i++ {
		child := e.Child(i)
		child.format(tp)
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
	buf.WriteString("columns:")

	// Write the required columns.
	if len(requiredProps.Projection.ordered) > 0 {
		for _, colIndex := range requiredProps.Projection.ordered {
			e.formatCol(&buf, colIndex, logicalProps.Relational.NotNullCols, " ")
		}
	} else {
		requiredProps.Projection.unordered.ForEach(func(i int) {
			e.formatCol(&buf, ColumnIndex(i), logicalProps.Relational.NotNullCols, " ")
		})
	}

	// Write the hidden columns.
	foundHidden := false
	logicalProps.Relational.OutputCols.ForEach(func(i int) {
		if !requiredProps.Projection.unordered.Contains(i) {
			if !foundHidden {
				e.formatCol(&buf, ColumnIndex(i), logicalProps.Relational.NotNullCols, " (")
				foundHidden = true
			} else {
				e.formatCol(&buf, ColumnIndex(i), logicalProps.Relational.NotNullCols, " ")
			}
		}
	})
	if foundHidden {
		buf.WriteString(")")
	}

	tp.Child(buf.String())

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

func (e *Expr) formatCol(buf *bytes.Buffer, colIndex ColumnIndex, notNullCols ColSet, separator string) {
	label := e.mem.metadata.ColumnLabel(colIndex)

	buf.WriteString(separator)
	buf.WriteString(label)
	buf.WriteString(":")
	fmt.Fprintf(buf, "%d", colIndex)
	if notNullCols.Contains(int(colIndex)) {
		buf.WriteString("*")
	}
}
