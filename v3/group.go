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
		extra:    3,
		children: []*expr{input, nil /* grouping */, nil /* projection */, nil /* filter */},
		props:    &relationalProps{},
	}
}

type groupBy struct{}

func (groupBy) kind() operatorKind {
	return relationalKind
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
	e.props.outerVars = g.requiredInputVars(e)
	e.props.outerVars &^= (e.props.outputVars | e.providedInputVars())
	for _, input := range e.inputs() {
		e.props.outerVars |= input.props.outerVars
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}

func (groupBy) requiredInputVars(e *expr) bitmap {
	var v bitmap
	for _, filter := range e.filters() {
		v |= filter.inputVars
	}
	for _, aggregate := range e.aggregations() {
		v |= aggregate.inputVars
	}
	for _, grouping := range e.groupings() {
		v |= grouping.inputVars
	}
	return v
}
