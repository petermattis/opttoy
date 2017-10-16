package v3

import (
	"bytes"
	"fmt"
)

func init() {
	operatorTab[unionOp] = operatorInfo{
		name: "union",

		format: func(e *expr, buf *bytes.Buffer, level int) {
			indent := spaces[:2*level]
			fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.table)
			e.formatVars(buf)
			buf.WriteString("\n")
			formatExprs(buf, "filters", e.filters(), level)
			formatExprs(buf, "inputs", e.inputs(), level)
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

	operatorTab[intersectOp] = operatorInfo{
		name: "intersect",
	}
	operatorTab[exceptOp] = operatorInfo{
		name: "except",
	}
}
