package v3

import (
	"bytes"
)

func init() {
	registerOperator(innerJoinOp, "inner join", join{})
	registerOperator(leftJoinOp, "left join", join{})
	registerOperator(rightJoinOp, "right join", join{})
	registerOperator(fullJoinOp, "full join", join{})
	registerOperator(semiJoinOp, "semi-join", join{})
	registerOperator(antiJoinOp, "anti-join", join{})
}

type join struct{}

func (join) kind() operatorKind {
	return relationalKind
}

func (join) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (join) updateProps(e *expr) {
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}

	e.props.notNullCols = 0
	var providedInputVars bitmap
	for _, input := range e.inputs() {
		e.props.notNullCols |= input.props.notNullCols
		outputVars := input.props.outputVars()
		input.props.requiredOutputVars = outputVars
		providedInputVars |= outputVars
	}

	e.inputVars &^= (e.props.outputVars() | providedInputVars)
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	e.props.applyFilters(e.filters())
}
