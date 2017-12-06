package v3

func init() {
	registerOperator(variableOp, "variable", variable{})
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

type variable struct{}

func (variable) kind() operatorKind {
	// Variable is both a logical and a physical operator.
	return logicalKind | physicalKind | scalarKind
}

func (variable) layout() exprLayout {
	return exprLayout{}
}

func (variable) format(e *expr, tp *treePrinter) {
	tp.Addf("%v (%s) [in=%s]", e.op, e.private, e.scalarProps.inputCols)
}

func (variable) initKeys(e *expr, state *queryState) {
}

func (variable) updateProps(e *expr) {
}

func (variable) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
