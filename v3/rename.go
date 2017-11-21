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
		children: []*expr{input},
	}
}

type rename struct{}

func (rename) kind() operatorKind {
	return relationalKind
}

func (rename) layout() exprLayout {
	return exprLayout{}
}

func (rename) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (rename) initKeys(e *expr, state *queryState) {
}

func (rename) updateProps(e *expr) {
	// Rename is pass through and requires any input columns that its inputs
	// require.
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}

func (rename) requiredProps(required *physicalProps, child int) *physicalProps {
	return required // pass through
}
