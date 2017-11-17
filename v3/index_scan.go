package v3

import "bytes"

func init() {
	registerOperator(indexScanOp, "index scan", indexScan{})
}

type indexScan struct{}

func (indexScan) kind() operatorKind {
	return relationalKind
}

func (indexScan) layout() exprLayout {
	return exprLayout{}
}

func (indexScan) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
}

func (indexScan) initKeys(e *expr, state *queryState) {
}

func (indexScan) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
}
