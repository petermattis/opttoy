package v3

func init() {
	operatorTab[scanOp] = operatorInfo{
		name: "scan",
		updateProperties: func(expr *expr) {
			expr.outputVars = expr.inputVars
		},
	}
}
