package v3

func init() {
	registerOperator(groupByOp, "group-by", groupByClass{})
}

func newGroupByExpr(input *expr) *expr {
	return &expr{
		op:       groupByOp,
		children: []*expr{input, nil /* grouping */, nil /* projection */},
	}
}

type groupByClass struct{}

var _ operatorClass = groupByClass{}

func (groupByClass) kind() operatorKind {
	return logicalKind | relationalKind
}

func (groupByClass) layout() exprLayout {
	return exprLayout{
		groupings:    1,
		aggregations: 2,
	}
}

func (groupByClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "groupings", e.groupings())
	formatExprs(tp, "aggregations", e.aggregations())
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (groupByClass) initKeys(e *expr, state *queryState) {
}

func (groupByClass) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
	e.props.outerCols.DifferenceWith(e.providedInputCols())
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}

func (groupByClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
