package v3

import (
	"bytes"
)

func init() {
	registerOperator(unionOp, "union", union{})
	registerOperator(intersectOp, "intersect", nil)
	registerOperator(exceptOp, "except", nil)
}

func newSetExpr(op operator, input1, input2 *expr) *expr {
	return &expr{
		op:       op,
		extra:    1,
		children: []*expr{input1, input2, nil /* filter */},
		props:    &relationalProps{},
	}
}

type union struct{}

func (union) kind() operatorKind {
	return relationalKind
}

func (union) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (union) initKeys(e *expr, state *queryState) {
}

func (u union) updateProps(e *expr) {
	// Union is pass through and requires any input variables that its inputs
	// require.
	e.props.outerVars = 0
	for _, input := range e.inputs() {
		e.props.outerVars |= input.props.outerVars
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}

func (union) requiredInputVars(e *expr) bitmap {
	return e.providedInputVars()
}
