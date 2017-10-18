package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(variableOp, "variable", variable{})
}

type variable struct{}

func (variable) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props.state.getData(e.dataIndex))
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (variable) updateProperties(e *expr) {
	// Variables are "pass through": the output variables are the same as the
	// input variables.
	e.outputVars = e.inputVars
}
