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
		extra:    1,
		children: []*expr{left, right, nil /* filter */},
	}
}

func newJoinPattern(op operator, left, right, filter *expr) *expr {
	return &expr{
		op:       op,
		extra:    1,
		children: []*expr{left, right, filter},
	}
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
		e.props.notNullCols.unionWith(input.props.notNullCols)
	}

	e.props.joinDepth = 1
	e.props.outerVars = j.requiredInputVars(e)
	e.props.outerVars &^= (e.props.outputVars | e.providedInputVars())
	for _, input := range e.inputs() {
		e.props.outerVars.unionWith(input.props.outerVars)
		e.props.joinDepth += input.props.joinDepth
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}

func (join) requiredInputVars(e *expr) bitmap {
	var v bitmap
	for _, filter := range e.filters() {
		v.unionWith(filter.scalarInputVars())
	}
	return v
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
