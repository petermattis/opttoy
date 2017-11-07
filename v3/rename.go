package v3

import (
	"bytes"
)

func init() {
	registerOperator(renameOp, "rename", rename{})
}

type rename struct{}

func (rename) kind() operatorKind {
	return relationalKind
}

func (rename) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (r rename) updateProps(e *expr) {
	// Rename is pass through and requires any input variables that its inputs
	// require.
	e.inputVars = 0
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}

	// TODO(peter): update keys
}

func (rename) requiredInputVars(e *expr) bitmap {
	return e.providedInputVars()
}
