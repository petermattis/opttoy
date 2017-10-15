package v3

func init() {
	operatorTab[selectOp] = operatorInfo{
		name: "selectOp",
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
