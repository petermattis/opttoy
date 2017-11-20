package v3

import "bytes"

func init() {
	registerOperator(indexJoinOp, "index join", indexJoin{})
}

type indexJoin struct{}

func (indexJoin) kind() operatorKind {
	return relationalKind
}

func (indexJoin) layout() exprLayout {
	return exprLayout{}
}

func (indexJoin) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
}

func (indexJoin) initKeys(e *expr, state *queryState) {
}

func (indexJoin) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
}

func (indexJoin) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
