package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(orderByOp, "order-by", orderBy{})
}

func newOrderByExpr(input *expr) *expr {
	return &expr{
		op:       orderByOp,
		children: []*expr{input},
	}
}

type orderBy struct{}

func (orderBy) kind() operatorKind {
	return relationalKind
}

func (orderBy) layout() exprLayout {
	return exprLayout{}
}

func (orderBy) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (orderBy) initKeys(e *expr, state *queryState) {
}

func (orderBy) updateProps(e *expr) {
}

func (orderBy) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}

// orderSpec defines the ordering of columns provided or required by a
// relation. A negative value indicates descending ordering on the column index
// "-(value+1)".
type ordering []bitmapIndex

func (o ordering) String() string {
	var buf bytes.Buffer
	o.format(&buf)
	return buf.String()
}

func (o ordering) format(buf *bytes.Buffer) {
	for i, col := range o {
		if i > 0 {
			buf.WriteString(",")
		}
		if col >= 0 {
			fmt.Fprintf(buf, "+%d", col)
		} else {
			fmt.Fprintf(buf, "-%d", -(col + 1))
		}
	}
}

// Provides returns true iff the receiver is a prefix of the required ordering.
func (o ordering) provides(required ordering) bool {
	if len(o) < len(required) {
		return false
	}
	for i := range required {
		if o[i] != required[i] {
			return false
		}
	}
	return true
}
