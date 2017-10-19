package v3

import (
	"bytes"
)

func init() {
	registerOperator(unionOp, "union", union{})
	registerOperator(intersectOp, "intersect", nil)
	registerOperator(exceptOp, "except", nil)
}

type union struct{}

func (union) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (union) updateProps(e *expr) {
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	// TODO(peter): update expr.props.
}
