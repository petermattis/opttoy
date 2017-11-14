package v3

import (
	"bytes"
)

func init() {
	registerOperator(orderByOp, "orderBy", orderBy{})
}

func newOrderByExpr(input *expr) *expr {
	return &expr{
		op:       orderByOp,
		children: []*expr{input},
	}
}

type orderBy struct{}

func (orderBy) kind() operatorKind {
	return relationalKind
}

func (orderBy) layout() exprLayout {
	return exprLayout{
		numAux: 0,
	}
}

func (orderBy) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	// formatExprs(buf, "sorting", e.sortings(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (orderBy) initKeys(e *expr, state *queryState) {
}

func (orderBy) updateProps(e *expr) {
	unimplemented("%s.updateProperties", e.op)
}
