package v3

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func init() {
	registerOperator(variableOp, "variable", variable{})
}

type variable struct{}

func (variable) kind() operatorKind {
	return scalarKind
}

func (variable) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.private)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (variable) initKeys(e *expr, state *queryState) {
}

func (variable) updateProps(e *expr) {
}

func (variable) requiredInputVars(e *expr) bitmap {
	return 0
}

func (variable) equal(a, b *expr) bool {
	aCol := a.private.(*parser.ColumnItem)
	bCol := b.private.(*parser.ColumnItem)
	return aCol.TableName == bCol.TableName &&
		aCol.ColumnName == bCol.ColumnName
}
