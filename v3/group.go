package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(groupByOp, "groupBy", groupBy{})
}

type groupBy struct{}

func (groupBy) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "groupings", e.groupings(), level)
	formatExprs(buf, "aggregations", e.aggregations(), level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (groupBy) updateProps(e *expr) {
	// TODO(peter): I haven't thought about this carefully. It is likely
	// incorrect.
	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}
	for _, aggregate := range e.aggregations() {
		e.inputVars |= aggregate.inputVars
	}
	for _, grouping := range e.groupings() {
		e.inputVars |= grouping.inputVars
	}
	e.outputVars = e.inputVars

	// TODO(peter): update expr.props.
}
