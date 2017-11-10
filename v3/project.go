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
		extra:    2,
		children: []*expr{input, nil /* projection */, nil /* filter */},
		props:    &logicalProps{},
	}
}

type project struct{}

func (project) kind() operatorKind {
	return relationalKind
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
	e.inputVars = p.requiredInputVars(e)
	e.inputVars &^= (e.props.outputVars() | e.providedInputVars())
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}

func (project) requiredInputVars(e *expr) bitmap {
	var v bitmap
	for _, filter := range e.filters() {
		v |= filter.inputVars
	}
	for _, project := range e.projections() {
		v |= project.inputVars
	}
	return v
}
