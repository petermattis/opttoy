package v3

func init() {
	operatorTab[unionOp] = operatorInfo{
		name: "union",
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
