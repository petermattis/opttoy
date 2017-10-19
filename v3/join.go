package v3

import (
	"bytes"
)

func init() {
	registerOperator(innerJoinOp, "inner join", innerJoin{})
	registerOperator(leftJoinOp, "left join", nil)
	registerOperator(rightJoinOp, "right join", nil)
	registerOperator(fullJoinOp, "full join", nil)
	registerOperator(semiJoinOp, "semi join", nil)
	registerOperator(antiJoinOp, "anti join", nil)
}

type innerJoin struct{}

func (innerJoin) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (innerJoin) updateProps(e *expr) {
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}
	props := e.props
	props.notNullCols = 0
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
		props.notNullCols |= input.props.notNullCols
	}
	e.outputVars = e.inputVars

	props.applyFilters(e.filters())
}
