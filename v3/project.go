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
		children: []*expr{input, nil /* projection */},
	}
}

type project struct{}

func (project) kind() operatorKind {
	return relationalKind
}

func (project) layout() exprLayout {
	return exprLayout{
		projections: 1,
	}
}

func (project) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "projections", e.projections(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
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
	return required // pass through
}
