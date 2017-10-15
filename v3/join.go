package v3

func init() {
	operatorTab[innerJoinOp] = operatorInfo{
		name: "inner join",
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
	operatorTab[leftJoinOp] = operatorInfo{name: "left join"}
	operatorTab[rightJoinOp] = operatorInfo{name: "right join"}
	operatorTab[crossJoinOp] = operatorInfo{name: "cross join"}
	operatorTab[semiJoinOp] = operatorInfo{name: "semi join"}
	operatorTab[antiJoinOp] = operatorInfo{name: "anti join"}
}
