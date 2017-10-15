package v3

import (
	"bytes"
	"fmt"
)

func init() {
	operatorTab[variableOp] = operatorInfo{
		name: "variable",

		format: func(e *expr, buf *bytes.Buffer, level int) {
			indent := spaces[:2*level]
			fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.body)
			e.formatVars(buf)
			buf.WriteString("\n")
			formatExprs(buf, "filters", e.filters(), level)
			formatExprs(buf, "inputs", e.inputs(), level)
		},

		updateProperties: func(expr *expr) {
			// Variables are "pass through": the output variables are the same as the
			// input variables.
			expr.outputVars = expr.inputVars
		},
	}
}
