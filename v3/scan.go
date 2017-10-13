package v3

func init() {
	operatorTab[scanOp] = operatorInfo{
		name: "scan",
		columns: func(expr *expr) []bitmapIndex {
			// The output columns are whatever our output variables are.
			return expr.outputVars.indexes()
		},
		updateProperties: func(expr *expr) {
			expr.outputVars = expr.inputVars
		},
	}
}
