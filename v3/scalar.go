package v3

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const spaces = "                                                                "

func init() {
	registerOperator(constOp, "const", scalar{})
	registerOperator(listOp, "list", scalar{})
	registerOperator(orderedListOp, "ordered-list", scalar{})
	registerOperator(existsOp, "exists", scalar{})
	registerOperator(andOp, "AND", scalar{})
	registerOperator(orOp, "OR", scalar{})
	registerOperator(notOp, "NOT", scalar{})
	registerOperator(eqOp, "eq", scalar{})
	registerOperator(ltOp, "lt", scalar{})
	registerOperator(gtOp, "gt", scalar{})
	registerOperator(leOp, "le", scalar{})
	registerOperator(geOp, "ge", scalar{})
	registerOperator(neOp, "ne", scalar{})
	registerOperator(inOp, "IN", scalar{})
	registerOperator(notInOp, "NOT-IN", scalar{})
	registerOperator(likeOp, "LIKE", scalar{})
	registerOperator(notLikeOp, "NOT-LIKE", scalar{})
	registerOperator(iLikeOp, "ILIKE", scalar{})
	registerOperator(notILikeOp, "NOT-ILIKE", scalar{})
	registerOperator(similarToOp, "SIMILAR-TO", scalar{})
	registerOperator(notSimilarToOp, "NOT-SIMILAR-TO", scalar{})
	registerOperator(regMatchOp, "regmatch", scalar{})
	registerOperator(notRegMatchOp, "not-regmatch", scalar{})
	registerOperator(regIMatchOp, "regimatch", scalar{})
	registerOperator(notRegIMatchOp, "not-regimatch", scalar{})
	registerOperator(isDistinctFromOp, "IS-DISTINCT-FROM", scalar{})
	registerOperator(isNotDistinctFromOp, "IS-NOT-DISTINCT-FROM", scalar{})
	registerOperator(isOp, "IS", scalar{})
	registerOperator(isNotOp, "IS-NOT", scalar{})
	registerOperator(anyOp, "ANY", scalar{})
	registerOperator(someOp, "SOME", scalar{})
	registerOperator(allOp, "ALL", scalar{})
	registerOperator(bitandOp, "bitand", scalar{})
	registerOperator(bitorOp, "bitor", scalar{})
	registerOperator(bitxorOp, "bitxor", scalar{})
	registerOperator(plusOp, "plus", scalar{})
	registerOperator(minusOp, "minux", scalar{})
	registerOperator(multOp, "mult", scalar{})
	registerOperator(divOp, "div", scalar{})
	registerOperator(floorDivOp, "floor-div", scalar{})
	registerOperator(modOp, "mod", scalar{})
	registerOperator(powOp, "pow", scalar{})
	registerOperator(concatOp, "concat", scalar{})
	registerOperator(lShiftOp, "lshift", scalar{})
	registerOperator(rShiftOp, "rshift", scalar{})
	registerOperator(unaryPlusOp, "unary-plus", scalar{})
	registerOperator(unaryMinusOp, "unary-minus", scalar{})
	registerOperator(unaryComplementOp, "complement", scalar{})
	registerOperator(functionOp, "func", scalar{})
}

func newConstExpr(private interface{}) *expr {
	return &expr{
		op:          constOp,
		scalarProps: &scalarProps{},
		private:     private,
	}
}

func newFunctionExpr(private interface{}, children []*expr) *expr {
	e := &expr{
		op:          functionOp,
		children:    children,
		scalarProps: &scalarProps{},
		private:     private,
	}
	e.updateProps()
	return e
}

func newUnaryExpr(op operator, input1 *expr) *expr {
	e := &expr{
		op:          op,
		children:    []*expr{input1},
		scalarProps: &scalarProps{},
	}
	e.updateProps()
	return e
}

func newBinaryExpr(op operator, input1, input2 *expr) *expr {
	e := &expr{
		op:          op,
		children:    []*expr{input1, input2},
		scalarProps: &scalarProps{},
	}
	e.updateProps()
	return e
}

type scalar struct{}

func (scalar) kind() operatorKind {
	return scalarKind
}

func (scalar) layout() exprLayout {
	return exprLayout{}
}

func (scalar) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v", indent, e.op)
	if e.private != nil {
		fmt.Fprintf(buf, " (%s)", e.private)
	}
	if e.scalarProps != nil && !e.scalarProps.inputCols.Empty() {
		fmt.Fprintf(buf, " [in=%s]", e.scalarProps.inputCols)
	}
	buf.WriteString("\n")
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (scalar) initKeys(e *expr, state *queryState) {
}

func (scalar) updateProps(e *expr) {
	if e.scalarProps != nil {
		// For a scalar operation the required input columns is the union of the
		// input columns of its inputs.
		e.scalarProps.inputCols = bitmap{}
		for _, input := range e.inputs() {
			e.scalarProps.inputCols.UnionWith(input.scalarInputCols())
		}
	}
}

func (scalar) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}

func substitute(e *expr, columns bitmap, replacement *expr) *expr {
	if e.op == variableOp {
		if e.scalarInputCols() == columns {
			return replacement
		}
		return e
	}

	result := *e
	result.children = make([]*expr, len(e.children))
	copy(result.children, e.children)
	result.scalarProps = &scalarProps{}

	inputs := result.inputs()
	for i, input := range inputs {
		inputs[i] = substitute(input, columns, replacement)
	}
	result.updateProps()
	return &result
}

func normalize(e *expr) {
	if e == nil {
		return
	}
	normalizeScalarEq(e)
	for _, input := range e.children {
		normalize(input)
	}
}

func normalizeScalarEq(e *expr) {
	if e.op != eqOp {
		return
	}

	left := e.children[0]
	right := e.children[1]

	// Normalize "a = b" such that the variable with the lower index is on the
	// left side of the equality.
	if left.op == variableOp && right.op == variableOp {
		leftCol := left.private.(columnProps)
		rightCol := right.private.(columnProps)
		if leftCol.index > rightCol.index {
			e.children[0] = right
			e.children[1] = left
			return
		}
	}

	// Normalize "<expr> = <var>" to "<var> = <expr".
	if left.op != variableOp && right.op == variableOp {
		e.children[0] = right
		e.children[1] = left
		return
	}
}

func isAggregate(e *expr) bool {
	if e.op != functionOp {
		return false
	}
	if def, ok := e.private.(*tree.FunctionDefinition); ok {
		if strings.EqualFold(def.Name, "count") ||
			strings.EqualFold(def.Name, "min") ||
			strings.EqualFold(def.Name, "max") ||
			strings.EqualFold(def.Name, "sum") ||
			strings.EqualFold(def.Name, "avg") {
			return true
		}
	}
	return false
}

func containsAggregate(e *expr) bool {
	if isAggregate(e) {
		return true
	}
	for _, input := range e.inputs() {
		if containsAggregate(input) {
			return true
		}
	}
	return false
}
