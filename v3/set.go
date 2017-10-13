package v3

func init() {
	operatorTab[unionOp] = operatorInfo{
		name: "union",
		columns: func(expr *expr) []bitmapIndex {
			// The output columns are the same as the first input's columns.
			return expr.inputs()[0].columns()
		},
		updateProperties: func(expr *expr) {
			expr.inputVars = 0
			for _, filter := range expr.filters() {
				expr.inputVars |= filter.inputVars
			}
			for _, input := range expr.inputs() {
				expr.inputVars |= input.inputVars
			}
			expr.outputVars = expr.inputVars
		},
	}

	operatorTab[intersectOp] = operatorInfo{
		name: "intersect",
	}
	operatorTab[exceptOp] = operatorInfo{
		name: "except",
	}
}
