package v3

import (
	"bytes"
)

func init() {
	registerOperator(distinctOp, "distinct", distinct{})
}

type distinct struct{}

func (distinct) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (distinct) updateProps(e *expr) {
	unimplemented("distinct.updateProperties")
}
