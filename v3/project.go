package v3

func init() {
	operatorTab[projectOp] = operatorInfo{
		name: "projectOp",
		updateProperties: func(expr *expr) {
			expr.inputVars = 0
			for _, filter := range expr.filters() {
				expr.inputVars |= filter.inputVars
			}
			expr.outputVars = 0
			for _, project := range expr.projections() {
				expr.inputVars |= project.inputVars
				expr.outputVars |= project.outputVars
			}
		},
	}
}
