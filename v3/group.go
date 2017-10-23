package v3

import (
	"bytes"
)

func init() {
	registerOperator(groupByOp, "groupBy", groupBy{})
}

type groupBy struct{}

func (groupBy) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "groupings", e.groupings(), level)
	formatExprs(buf, "aggregations", e.aggregations(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (groupBy) updateProps(e *expr) {
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}
	for _, aggregate := range e.aggregations() {
		e.inputVars |= aggregate.inputVars
	}
	for _, grouping := range e.groupings() {
		e.inputVars |= grouping.inputVars
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
