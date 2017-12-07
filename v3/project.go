package v3

func init() {
	registerOperator(projectOp, "project", project{})
}

func newProjectExpr(input *expr) *expr {
	return &expr{
		op:       projectOp,
		children: []*expr{input, nil /* projection */},
	}
}

type project struct{}

func (project) kind() operatorKind {
	// Project is both a logical and a physical operator.
	return logicalKind | physicalKind | relationalKind
}

func (project) layout() exprLayout {
	return exprLayout{
		projections: 1,
	}
}

func (project) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "projections", e.projections())
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (project) initKeys(e *expr, state *queryState) {
}

func (project) updateProps(e *expr) {
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}

func (project) requiredProps(required *physicalProps, child int) *physicalProps {
	if child == 0 {
		return required // pass through
	}
	return nil
}
