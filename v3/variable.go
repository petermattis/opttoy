package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(variableOp, "variable", variable{})
}

func newVariableExpr(private interface{}) *expr {
	return &expr{
		op:      variableOp,
		private: private,
	}
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
}

func (variable) initKeys(e *expr, state *queryState) {
}

func (variable) updateProps(e *expr) {
}

func (variable) requiredInputVars(e *expr) bitmap {
	return 0
}
