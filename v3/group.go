package v3

import (
	"bytes"
)

func init() {
	registerOperator(groupByOp, "group-by", groupBy{})
}

func newGroupByExpr(input *expr) *expr {
	return &expr{
		op:       groupByOp,
		children: []*expr{input, nil /* grouping */, nil /* projection */},
	}
}

type groupBy struct{}

func (groupBy) kind() operatorKind {
	return logicalKind | relationalKind
}

func (groupBy) layout() exprLayout {
	return exprLayout{
		groupings:    1,
		aggregations: 2,
	}
}

func (groupBy) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "groupings", e.groupings(), level)
	formatExprs(buf, "aggregations", e.aggregations(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (groupBy) initKeys(e *expr, state *queryState) {
}

func (groupBy) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
	e.props.outerCols.DifferenceWith(e.providedInputCols())
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}

func (groupBy) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
