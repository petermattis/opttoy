package v3

func init() {
	registerOperator(unionOp, "union", unionClass{})
	registerOperator(intersectOp, "intersect", nil)
	registerOperator(exceptOp, "except", nil)
}

func newSetExpr(op operator, input1, input2 *expr) *expr {
	return &expr{
		op:       op,
		children: []*expr{input1, input2},
	}
}

type unionClass struct{}

var _ operatorClass = unionClass{}

func (unionClass) kind() operatorKind {
	return logicalKind | relationalKind
}

func (unionClass) layout() exprLayout {
	return exprLayout{}
}

func (unionClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (unionClass) initKeys(e *expr, state *queryState) {
}

func (unionClass) updateProps(e *expr) {
	// Union is pass through and requires any input columns that its inputs
	// require.
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}

func (unionClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
