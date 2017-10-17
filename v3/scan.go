package v3

import (
	"bytes"
	"fmt"
)

func init() {
	operatorTab[scanOp] = operatorInfo{
		name: "scan",

		format: func(e *expr, buf *bytes.Buffer, level int) {
			indent := spaces[:2*level]
			fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
			e.formatVars(buf)
			buf.WriteString("\n")
			formatExprs(buf, "filters", e.filters(), level)
			formatExprs(buf, "inputs", e.inputs(), level)
		},

		updateProperties: func(expr *expr) {
			expr.inputVars = 0
			for _, col := range expr.props.columns {
				expr.inputVars.set(col.index)
			}
			expr.outputVars = expr.inputVars
		},
	}
}
