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
	for _, project := range e.projections() {
		e.inputVars |= project.inputVars
	}
	var providedInputVars bitmap
	for _, input := range e.inputs() {
		outputVars := input.props.outputVars()
		providedInputVars |= outputVars
		input.props.requiredOutputVars = e.inputVars & outputVars
	}

	e.inputVars &^= (e.props.outputVars() | providedInputVars)
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	// TODO(peter): update expr.props.
}
