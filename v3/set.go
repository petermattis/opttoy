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
		children: []*expr{input1, input2},
	}
}

type union struct{}

func (union) kind() operatorKind {
	return relationalKind
}

func (union) layout() exprLayout {
	return exprLayout{}
}

func (union) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (union) initKeys(e *expr, state *queryState) {
}

func (union) updateProps(e *expr) {
	// Union is pass through and requires any input columns that its inputs
	// require.
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	e.props.applyInputs(e.inputs())
}
