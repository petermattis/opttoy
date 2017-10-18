package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(renameOp, "rename", rename{})
}

type rename struct{}

func (rename) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (rename) updateProps(e *expr) {
	e.inputVars = 0
	for _, input := range e.inputs() {
		e.inputVars |= input.outputVars
	}
	e.outputVars = e.inputVars

	// TODO(peter): update expr.props.
}
