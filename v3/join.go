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
	e.props.notNullCols = 0
	for _, input := range e.inputs() {
		for _, col := range input.props.columns {
			e.inputVars.set(col.index)
		}
		e.props.notNullCols |= input.props.notNullCols
	}
	e.props.applyFilters(e.filters())
}
