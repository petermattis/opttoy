package v3

import "bytes"

func init() {
	registerOperator(selectOp, "select", sel{})
}

func newSelectExpr(input *expr) *expr {
	return &expr{
		op:       selectOp,
		children: []*expr{input, nil /* filter */},
	}
}

type sel struct{}

func (sel) kind() operatorKind {
	return relationalKind
}

func (sel) layout() exprLayout {
	return exprLayout{
		filters: 1,
	}
}

func (sel) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (sel) initKeys(e *expr, state *queryState) {
}

func (sel) updateProps(e *expr) {
	// Select is pass through and requires any input columns that its inputs
	// require.
	excluded := e.props.outputCols.Union(e.providedInputCols())
	e.props.outerCols = e.requiredInputCols().Difference(excluded)
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
	}

	// Keys and foreign keys pass through directly from the input.
	e.props.weakKeys = e.children[0].props.weakKeys
	e.props.foreignKeys = e.children[0].props.foreignKeys

	e.props.applyFilters(e.filters())
	e.props.applyInputs(e.inputs())
}
