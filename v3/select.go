package v3

func init() {
	registerOperator(selectOp, "select", selectClass{})
}

func newSelectExpr(input *expr) *expr {
	return &expr{
		op:       selectOp,
		children: []*expr{input, nil /* filter */},
	}
}

type selectClass struct{}

var _ operatorClass = selectClass{}

func (selectClass) kind() operatorKind {
	// Select is both a logical and a physical operator.
	return logicalKind | physicalKind | relationalKind
}

func (selectClass) layout() exprLayout {
	return exprLayout{
		filters: 1,
	}
}

func (selectClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "filters", e.filters())
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (selectClass) initKeys(e *expr, state *queryState) {
}

func (selectClass) updateProps(e *expr) {
	// Select is pass through and requires any input columns that its inputs
	// require.
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	// Keys and foreign keys pass through directly from the input.
	e.props.weakKeys = e.children[0].props.weakKeys
	e.props.foreignKeys = e.children[0].props.foreignKeys

	e.props.applyFilters(e.filters())
	e.props.applyInputs(e.inputs())
}

func (selectClass) requiredProps(required *physicalProps, child int) *physicalProps {
	if child == 0 {
		return required // pass through
	}
	return nil
}
