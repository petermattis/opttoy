package v3

import (
	"bytes"
	"fmt"
	"math/bits"
)

func init() {
	registerOperator(innerJoinOp, "inner join", innerJoin{})
	registerOperator(leftJoinOp, "left join", nil)
	registerOperator(rightJoinOp, "right join", nil)
	registerOperator(fullJoinOp, "full join", nil)
	registerOperator(semiJoinOp, "semi join", nil)
	registerOperator(antiJoinOp, "anti join", nil)
}

type innerJoin struct{}

func (innerJoin) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (innerJoin) updateProperties(expr *expr) {
	expr.inputVars = 0
	for _, filter := range expr.filters() {
		expr.inputVars |= filter.inputVars
	}
	props := expr.props
	props.notNullCols = 0
	for _, input := range expr.inputs() {
		expr.inputVars |= input.inputVars
		props.notNullCols |= input.props.notNullCols
	}
	expr.outputVars = expr.inputVars

	// TODO(peter): update expr.props
	for _, filter := range expr.filters() {
		// TODO(peter): !isNullTolerant(filter)
		for v := filter.inputVars; v != 0; {
			i := uint(bits.TrailingZeros64(uint64(v)))
			v &^= 1 << i
			props.notNullCols |= 1 << i
		}
	}
}
