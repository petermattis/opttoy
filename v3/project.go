package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(projectOp, "project", project{})
}

type project struct{}

func (project) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "projections", e.projections(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (project) updateProperties(e *expr) {
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}
	e.outputVars = 0
	for _, project := range e.projections() {
		e.inputVars |= project.inputVars
		e.outputVars |= project.outputVars
	}

	// TODO(peter): update expr.props.
}
