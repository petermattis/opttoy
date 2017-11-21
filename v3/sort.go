package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(sortOp, "sort", sorter{})
}

type sortSpec struct {
	loc memoLoc
	// NB: the required ordering is specified in expr.physicalProperties.
}

func (s *sortSpec) String() string {
	return fmt.Sprintf("[%s]", s.loc)
}

type sorter struct{}

func (sorter) kind() operatorKind {
	return relationalKind
}

func (sorter) layout() exprLayout {
	return exprLayout{}
}

func (sorter) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
}

func (sorter) initKeys(e *expr, state *queryState) {
}

func (sorter) updateProps(e *expr) {
}

func (sorter) requiredProps(required *physicalProps, child int) *physicalProps {
	// A sort expression enforces ordering and does not require any specific
	// ordering from its input.
	return nil
}
