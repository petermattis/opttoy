package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(groupByOp, "groupBy", operatorInfo{
		format: func(e *expr, buf *bytes.Buffer, level int) {
			indent := spaces[:2*level]
			fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
			e.formatVars(buf)
			buf.WriteString("\n")
			formatExprs(buf, "groupings", e.groupings(), level)
			formatExprs(buf, "aggregations", e.aggregations(), level)
			formatExprs(buf, "filters", e.filters(), level)
			formatExprs(buf, "inputs", e.inputs(), level)
		},

		updateProperties: func(expr *expr) {
			// TODO(peter): I haven't thought about this carefully. It is likely
			// incorrect.
			expr.inputVars = 0
			for _, filter := range expr.filters() {
				expr.inputVars |= filter.inputVars
			}
			for _, aggregate := range expr.aggregations() {
				expr.inputVars |= aggregate.inputVars
			}
			for _, grouping := range expr.groupings() {
				expr.inputVars |= grouping.inputVars
			}
			expr.outputVars = expr.inputVars

			// TODO(peter): update expr.props.
		},
	})
}
