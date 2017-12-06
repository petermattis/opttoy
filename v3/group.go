package v3

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

func (groupBy) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "groupings", e.groupings())
	formatExprs(tp, "aggregations", e.aggregations())
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
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
