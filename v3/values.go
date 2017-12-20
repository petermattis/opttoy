package v3

import "github.com/cockroachdb/cockroach/pkg/util/treeprinter"

func init() {
	registerOperator(valuesOp, "values", valuesClass{})
}

type valuesClass struct{}

var _ operatorClass = valuesClass{}

func (valuesClass) kind() operatorKind {
	return logicalKind | relationalKind
}

func (valuesClass) layout() exprLayout {
	return exprLayout{}
}

func (valuesClass) format(e *expr, tp treeprinter.Node) {
	formatRelational(e, tp)
}

func (valuesClass) initKeys(e *expr, state *queryState) {
}

func (valuesClass) updateProps(e *expr) {
}

func (valuesClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
