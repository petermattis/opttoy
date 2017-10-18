package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(selectOp, "select", selectx{})
}

type selectx struct{}

func (selectx) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (selectx) updateProperties(expr *expr) {
	expr.inputVars = 0
	for _, filter := range expr.filters() {
		expr.inputVars |= filter.inputVars
	}
	for _, input := range expr.inputs() {
		expr.inputVars |= input.inputVars
	}
	expr.outputVars = expr.inputVars

	// TODO(peter): update expr.props.
}
