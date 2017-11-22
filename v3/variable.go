package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(variableOp, "variable", variable{})
}

func newVariableExpr(private interface{}, index bitmapIndex) *expr {
	e := &expr{
		op:          variableOp,
		scalarProps: &scalarProps{},
		private:     private,
	}
	e.scalarProps.inputCols.Add(index)
	e.updateProps()
	return e
}

type variable struct{}

func (variable) kind() operatorKind {
	// Variable is both a logical and a physical operator.
	return logicalKind | physicalKind | scalarKind
}

func (variable) layout() exprLayout {
	return exprLayout{}
}

func (variable) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s) [in=%s]",
		indent, e.op, e.private, e.scalarProps.inputCols)
	buf.WriteString("\n")
}

func (variable) initKeys(e *expr, state *queryState) {
}

func (variable) updateProps(e *expr) {
}

func (variable) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
