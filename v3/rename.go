package v3

func init() {
	operatorTab[renameOp] = operatorInfo{
		name: "rename",
		updateProperties: func(expr *expr) {
			expr.inputVars = 0
			for _, input := range expr.inputs() {
				expr.inputVars |= input.outputVars
			}
			expr.outputVars = expr.inputVars
		},
	}
}
