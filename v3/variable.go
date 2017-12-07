package v3

func init() {
	registerOperator(variableOp, "variable", variableClass{})
}

func newVariableExpr(private interface{}, index bitmapIndex) *expr {
	e := &expr{
		op:          variableOp,
		scalarProps: &scalarProps{},
		private:     private,
	}
	e.scalarProps.inputCols.Add(index)
	e.updateProps()
	return e
}

type variableClass struct{}

var _ operatorClass = variableClass{}

func (variableClass) kind() operatorKind {
	// Variable is both a logical and a physical operator.
	return logicalKind | physicalKind | scalarKind
}

func (variableClass) layout() exprLayout {
	return exprLayout{}
}

func (variableClass) format(e *expr, tp *treePrinter) {
	tp.Addf("%v (%s) [in=%s]", e.op, e.private, e.scalarProps.inputCols)
}

func (variableClass) initKeys(e *expr, state *queryState) {
}

func (variableClass) updateProps(e *expr) {
}

func (variableClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
