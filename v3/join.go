package v3

import "github.com/cockroachdb/cockroach/pkg/util/treeprinter"

func init() {
	registerOperator(innerJoinOp, "inner-join", joinClass{})
	registerOperator(leftJoinOp, "left-join", joinClass{})
	registerOperator(rightJoinOp, "right-join", joinClass{})
	registerOperator(fullJoinOp, "full-join", joinClass{})
	registerOperator(semiJoinOp, "semi-join", joinClass{})
	registerOperator(antiJoinOp, "anti-join", joinClass{})
}

func newJoinExpr(op operator, left, right *expr) *expr {
	return &expr{
		op:       op,
		children: []*expr{left, right, nil /* filter */},
	}
}

type joinClass struct{}

var _ operatorClass = joinClass{}

func (joinClass) kind() operatorKind {
	return logicalKind | relationalKind
}

func (joinClass) layout() exprLayout {
	return exprLayout{
		filters: 2,
	}
}

func (joinClass) format(e *expr, tp treeprinter.Node) {
	n := formatRelational(e, tp)
	formatExprs(n, "filters", e.filters())
	formatExprs(n, "inputs", e.inputs())
}

func (joinClass) initKeys(e *expr, state *queryState) {
}

func (joinClass) updateProps(e *expr) {
	e.props.notNullCols = bitmap{}
	for _, input := range e.inputs() {
		e.props.notNullCols.UnionWith(input.props.notNullCols)
	}

	e.props.joinDepth = 1
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
	e.props.outerCols.DifferenceWith(e.providedInputCols())
	for _, input := range e.inputs() {
		e.props.outerCols.UnionWith(input.props.outerCols)
		e.props.joinDepth += input.props.joinDepth
	}

	e.props.applyFilters(e.filters())
	e.props.applyInputs(e.inputs())
}

func (joinClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}

func joinOp(s string) operator {
	switch s {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		return innerJoinOp
	case "LEFT JOIN":
		return leftJoinOp
	case "RIGHT JOIN":
		return rightJoinOp
	case "FULL JOIN":
		return fullJoinOp
	default:
		unimplemented("unsupported JOIN type %s", s)
		return unknownOp
	}
}
