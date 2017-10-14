package v3

import "math/bits"

func init() {
	operatorTab[projectOp] = operatorInfo{
		name: "projectOp",
		columns: func(expr *expr) []bitmapIndex {
			projections := expr.projections()
			r := make([]bitmapIndex, len(projections))
			for i, project := range projections {
				r[i] = bitmapIndex(bits.TrailingZeros64(uint64(project.outputVars)))
			}
			return r
		},
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
