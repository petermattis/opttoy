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
		children: []*expr{input1, input2, nil /* filter */},
	}
}

type union struct{}

func (union) kind() operatorKind {
	return relationalKind
}

func (union) layout() exprLayout {
	return exprLayout{
		numAux:  1,
		filters: 2,
	}
}

func (union) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (union) initKeys(e *expr, state *queryState) {
}

func (u union) updateProps(e *expr) {
	// Union is pass through and requires any input columns that its inputs
	// require.
	e.props.outerCols = 0
	for _, input := range e.inputs() {
		e.props.outerCols.unionWith(input.props.outerCols)
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}
