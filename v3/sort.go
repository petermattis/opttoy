package v3

func init() {
	registerOperator(sortOp, "sort", sorter{})
}

type sorter struct{}

func (sorter) kind() operatorKind {
	return physicalKind | relationalKind
}

func (sorter) layout() exprLayout {
	return exprLayout{}
}

func (sorter) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	tp.Enter()
	formatExprs(tp, "inputs", e.inputs())
	tp.Exit()
}

func (sorter) initKeys(e *expr, state *queryState) {
}

func (sorter) updateProps(e *expr) {
}

func (sorter) requiredProps(required *physicalProps, child int) *physicalProps {
	// A sort expression enforces ordering and does not require any specific
	// ordering from its input.
	return nil
}
