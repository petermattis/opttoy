package v3

import (
	"bytes"
	"fmt"
)

func init() {
	operatorTab[renameOp] = operatorInfo{
		name: "rename",

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
			for _, input := range expr.inputs() {
				expr.inputVars |= input.outputVars
			}
			expr.outputVars = expr.inputVars

			// TODO(peter): update expr.props.
		},
	}
}
