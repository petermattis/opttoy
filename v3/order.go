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
		extra:    0,
		children: []*expr{input},
		props:    &logicalProps{},
	}
}

type orderBy struct{}

func (orderBy) kind() operatorKind {
	return relationalKind
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

func (orderBy) requiredInputVars(e *expr) bitmap {
	unimplemented("%s.requiredInputVars", e.op)
	return 0
}

func (orderBy) equal(a, b *expr) bool {
	return false
}
