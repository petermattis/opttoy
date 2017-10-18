package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(orderByOp, "orderBy", orderBy{})
}

type orderBy struct{}

func (orderBy) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	// formatExprs(buf, "sorting", e.sortings(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (orderBy) updateProps(e *expr) {
	unimplemented("%s.updateProperties", e.op)
}
