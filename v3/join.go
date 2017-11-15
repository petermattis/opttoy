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

func newJoinExpr(op operator, left, right *expr) *expr {
	return &expr{
		op:       op,
		children: []*expr{left, right, nil /* filter */},
	}
}

type join struct{}

func (join) kind() operatorKind {
	return relationalKind
}

func (join) layout() exprLayout {
	return exprLayout{
		numAux:  1,
		filters: 2,
	}
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
		e.props.notNullCols.unionWith(input.props.notNullCols)
	}

	e.props.joinDepth = 1
	e.props.outerCols = e.requiredInputCols()
	e.props.outerCols &^= (e.props.outputCols | e.providedInputCols())
	for _, input := range e.inputs() {
		e.props.outerCols.unionWith(input.props.outerCols)
		e.props.joinDepth += input.props.joinDepth
	}

	e.props.applyFilters(e.filters())
	e.props.applyInputs(e.inputs())

	// TODO(peter): update keys
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
