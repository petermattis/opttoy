package v3

import (
	"bytes"
)

func init() {
	registerOperator(renameOp, "rename", rename{})
}

type rename struct{}

func (rename) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (rename) updateProps(e *expr) {
	e.inputVars = 0
	for _, input := range e.inputs() {
		for _, col := range input.props.columns {
			e.inputVars.set(col.index)
		}
	}

	// TODO(peter): update expr.props.
}
