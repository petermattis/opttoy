package v3

func init() {
	registerOperator(projectOp, "project", projectClass{})
}

func newProjectExpr(input *expr) *expr {
	return &expr{
		op:       projectOp,
		children: []*expr{input, nil /* projection */},
	}
}

type projectClass struct{}

var _ operatorClass = projectClass{}

func (projectClass) kind() operatorKind {
	// Project is both a logical and a physical operator.
	return logicalKind | physicalKind | relationalKind
}

func (projectClass) layout() exprLayout {
	return exprLayout{
		projections: 1,
	}
}

func (projectClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "projections", e.projections())
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (projectClass) initKeys(e *expr, state *queryState) {
}

func (projectClass) updateProps(e *expr) {
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}

func (projectClass) requiredProps(required *physicalProps, child int) *physicalProps {
	if child == 0 {
		return required // pass through
	}
	return nil
}
