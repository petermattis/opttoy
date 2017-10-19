package v3

import (
	"bytes"
)

func init() {
	registerOperator(projectOp, "project", project{})
}

type project struct{}

func (project) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "projections", e.projections(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (project) updateProps(e *expr) {
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
