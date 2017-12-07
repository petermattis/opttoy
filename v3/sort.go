package v3

func init() {
	registerOperator(sortOp, "sort", sorterClass{})
}

type sorterClass struct{}

var _ operatorClass = sorterClass{}

func (sorterClass) kind() operatorKind {
	return physicalKind | relationalKind
}

func (sorterClass) layout() exprLayout {
	return exprLayout{}
}

func (sorterClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (sorterClass) initKeys(e *expr, state *queryState) {
}

func (sorterClass) updateProps(e *expr) {
}

func (sorterClass) requiredProps(required *physicalProps, child int) *physicalProps {
	// A sort expression enforces ordering and does not require any specific
	// ordering from its input.
	return nil
}
