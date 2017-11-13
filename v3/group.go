package v3

import (
	"bytes"
)

func init() {
	registerOperator(groupByOp, "groupBy", groupBy{})
}

func newGroupByExpr(input *expr) *expr {
	return &expr{
		op:       groupByOp,
		children: []*expr{input, nil /* grouping */, nil /* projection */, nil /* filter */},
	}
}

type groupBy struct{}

func (groupBy) kind() operatorKind {
	return relationalKind
}

func (groupBy) layout() exprLayout {
	return exprLayout{
		numAux:       3,
		groupings:    1,
		aggregations: 2,
		filters:      3,
	}
}

func (groupBy) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "groupings", e.groupings(), level)
	formatExprs(buf, "aggregations", e.aggregations(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (groupBy) initKeys(e *expr, state *queryState) {
}

func (g groupBy) updateProps(e *expr) {
	e.props.outerCols = g.requiredInputCols(e)
	e.props.outerCols &^= (e.props.outputCols | e.providedInputCols())
	for _, input := range e.inputs() {
		e.props.outerCols.unionWith(input.props.outerCols)
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}

func (groupBy) requiredInputCols(e *expr) bitmap {
	var v bitmap
	for _, filter := range e.filters() {
		v.unionWith(filter.scalarInputCols())
	}
	for _, aggregate := range e.aggregations() {
		v.unionWith(aggregate.scalarInputCols())
	}
	for _, grouping := range e.groupings() {
		v.unionWith(grouping.scalarInputCols())
	}
	return v
}
