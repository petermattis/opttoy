package v3

import "github.com/cockroachdb/cockroach/pkg/util/treeprinter"

// TODO(peter): This should be a table with 1 row and 0 columns to match
// current cockroach behavior.
var emptyRow = &expr{
	op:    valuesOp,
	props: &relationalProps{},
}

func init() {
	registerOperator(valuesOp, "values", valuesClass{})
}

type valuesClass struct{}

var _ operatorClass = valuesClass{}

func (valuesClass) kind() operatorKind {
	return logicalKind | physicalKind | relationalKind
}

func (valuesClass) layout() exprLayout {
	return exprLayout{}
}

func (valuesClass) format(e *expr, tp treeprinter.Node) {
	if e == emptyRow {
		tp.Childf("emptyrow")
		return
	}

	n := formatRelational(e, tp)
	if rows, ok := e.private.(*expr); ok {
		rows.format(n)
	}
}

func (valuesClass) initKeys(e *expr, state *queryState) {
}

func (valuesClass) updateProps(e *expr) {
}

func (valuesClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
