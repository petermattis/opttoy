package v3

func init() {
	operatorTab[variableOp] = operatorInfo{
		name: "variable",
		columns: func(expr *expr) []bitmapIndex {
			return nil
		},
		updateProperties: func(expr *expr) {
			// Variables are "pass through": the output variables are the same as the
			// input variables.
			expr.outputVars = expr.inputVars
		},
	}
}
