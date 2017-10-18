package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(distinctOp, "distinct", distinct{})
}

type distinct struct{}

func (distinct) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (distinct) updateProperties(e *expr) {
	unimplemented("distinct.updateProperties")
}
