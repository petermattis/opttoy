package v3

func init() {
	operatorTab[renameOp] = operatorInfo{
		name: "rename",
		columns: func(expr *expr) []bitmapIndex {
			// The output columns are whatever our output variables are.
			return expr.outputVars.indexes()
		},
		updateProperties: func(expr *expr) {
			expr.inputVars = 0
			for _, input := range expr.inputs() {
				expr.inputVars |= input.outputVars
			}
			expr.outputVars = expr.inputVars
		},
	}
}
