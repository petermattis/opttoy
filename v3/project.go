package v3

import (
	"bytes"
)

func init() {
	registerOperator(projectOp, "project", project{})
}

func newProjectExpr(input *expr) *expr {
	return &expr{
		op:       projectOp,
		children: []*expr{input, nil /* projection */, nil /* filter */},
	}
}

type project struct{}

func (project) kind() operatorKind {
	return relationalKind
}

func (project) layout() exprLayout {
	return exprLayout{
		numAux:      2,
		projections: 1,
		filters:     2,
	}
}

func (project) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "projections", e.projections(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (project) initKeys(e *expr, state *queryState) {
}

func (p project) updateProps(e *expr) {
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyFilters(e.filters())
	e.props.applyInputs(e.inputs())
}
