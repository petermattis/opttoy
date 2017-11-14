package v3

import (
	"bytes"
)

func init() {
	registerOperator(renameOp, "rename", rename{})
}

func newRenameExpr(input *expr) *expr {
	return &expr{
		op:       renameOp,
		children: []*expr{input, nil /* filter */},
	}
}

type rename struct{}

func (rename) kind() operatorKind {
	return relationalKind
}

func (rename) layout() exprLayout {
	return exprLayout{
		numAux:  1,
		filters: 1,
	}
}

func (rename) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (rename) initKeys(e *expr, state *queryState) {
}

func (r rename) updateProps(e *expr) {
	// Rename is pass through and requires any input columns that its inputs
	// require.
	e.props.outerCols = 0
	for _, input := range e.inputs() {
		e.props.outerCols.unionWith(input.props.outerCols)
	}

	e.props.applyFilters(e.filters())

	// TODO(peter): update keys
}
