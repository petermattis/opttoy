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
	for _, input := range e.inputs() {
		var inputVars bitmap
		for _, col := range input.props.columns {
			inputVars.set(col.index)
		}
		input.props.requiredOutputVars = inputVars
		e.inputVars |= inputVars
	}

	// TODO(peter): update expr.props.
}
