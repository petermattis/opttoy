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

func (join) initKeys(e *expr, state *queryState) {
}

func (j join) updateProps(e *expr) {
	e.props.notNullCols = 0
	for _, input := range e.inputs() {
		e.props.notNullCols |= input.props.notNullCols
	}

	e.inputVars = j.requiredInputVars(e)
	e.inputVars &^= (e.props.outputVars() | e.providedInputVars())
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}

func (join) requiredInputVars(e *expr) bitmap {
	var v bitmap
	for _, filter := range e.filters() {
		v |= filter.inputVars
	}
	return v
}

func (join) equal(a, b *expr) bool {
	return true
}

func joinOp(s string) operator {
	switch s {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		return innerJoinOp
	case "LEFT JOIN":
		return leftJoinOp
	case "RIGHT JOIN":
		return rightJoinOp
	case "FULL JOIN":
		return fullJoinOp
	default:
		unimplemented("unsupported JOIN type %s", s)
		return unknownOp
	}
}
