package v3

func init() {
	operatorTab[selectOp] = operatorInfo{
		name: "selectOp",
		columns: func(expr *expr) []bitmapIndex {
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
}
