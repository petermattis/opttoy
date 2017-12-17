package v3

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

const spaces = "                                                                "

func init() {
	registerOperator(constOp, "const", scalarClass{})
	registerOperator(placeholderOp, "placeholder", scalarClass{})
	registerOperator(listOp, "list", scalarClass{})
	registerOperator(orderedListOp, "ordered-list", scalarClass{})
	registerOperator(existsOp, "exists", scalarClass{})
	registerOperator(andOp, "AND", scalarClass{})
	registerOperator(orOp, "OR", scalarClass{})
	registerOperator(notOp, "NOT", scalarClass{})
	registerOperator(eqOp, "eq", scalarClass{})
	registerOperator(ltOp, "lt", scalarClass{})
	registerOperator(gtOp, "gt", scalarClass{})
	registerOperator(leOp, "le", scalarClass{})
	registerOperator(geOp, "ge", scalarClass{})
	registerOperator(neOp, "ne", scalarClass{})
	registerOperator(inOp, "IN", scalarClass{})
	registerOperator(notInOp, "NOT-IN", scalarClass{})
	registerOperator(likeOp, "LIKE", scalarClass{})
	registerOperator(notLikeOp, "NOT-LIKE", scalarClass{})
	registerOperator(iLikeOp, "ILIKE", scalarClass{})
	registerOperator(notILikeOp, "NOT-ILIKE", scalarClass{})
	registerOperator(similarToOp, "SIMILAR-TO", scalarClass{})
	registerOperator(notSimilarToOp, "NOT-SIMILAR-TO", scalarClass{})
	registerOperator(regMatchOp, "regmatch", scalarClass{})
	registerOperator(notRegMatchOp, "not-regmatch", scalarClass{})
	registerOperator(regIMatchOp, "regimatch", scalarClass{})
	registerOperator(notRegIMatchOp, "not-regimatch", scalarClass{})
	registerOperator(isDistinctFromOp, "IS-DISTINCT-FROM", scalarClass{})
	registerOperator(isNotDistinctFromOp, "IS-NOT-DISTINCT-FROM", scalarClass{})
	registerOperator(isOp, "IS", scalarClass{})
	registerOperator(isNotOp, "IS-NOT", scalarClass{})
	registerOperator(anyOp, "ANY", scalarClass{})
	registerOperator(someOp, "SOME", scalarClass{})
	registerOperator(allOp, "ALL", scalarClass{})
	registerOperator(bitandOp, "bitand", scalarClass{})
	registerOperator(bitorOp, "bitor", scalarClass{})
	registerOperator(bitxorOp, "bitxor", scalarClass{})
	registerOperator(plusOp, "plus", scalarClass{})
	registerOperator(minusOp, "minux", scalarClass{})
	registerOperator(multOp, "mult", scalarClass{})
	registerOperator(divOp, "div", scalarClass{})
	registerOperator(floorDivOp, "floor-div", scalarClass{})
	registerOperator(modOp, "mod", scalarClass{})
	registerOperator(powOp, "pow", scalarClass{})
	registerOperator(concatOp, "concat", scalarClass{})
	registerOperator(lShiftOp, "lshift", scalarClass{})
	registerOperator(rShiftOp, "rshift", scalarClass{})
	registerOperator(unaryPlusOp, "unary-plus", scalarClass{})
	registerOperator(unaryMinusOp, "unary-minus", scalarClass{})
	registerOperator(unaryComplementOp, "complement", scalarClass{})
	registerOperator(functionOp, "func", scalarClass{})
}

var null = func() *expr {
	e := newConstExpr(tree.DNull)
	e.scalarProps.typ = types.Null
	return e
}()

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

type scalarClass struct{}

var _ operatorClass = scalarClass{}

func (scalarClass) kind() operatorKind {
	// Scalar is both a logical and a physical operator.
	return logicalKind | physicalKind | scalarKind
}

func (scalarClass) layout() exprLayout {
	return exprLayout{}
}

func (scalarClass) format(e *expr, tp treeprinter.Node) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", e.op)
	if e.private != nil {
		fmt.Fprintf(&buf, " (%s)", e.private)
	}
	if e.scalarProps != nil {
		buf.WriteString(" [")
		var sep string
		if !e.scalarProps.inputCols.Empty() {
			fmt.Fprintf(&buf, "in=%s", e.scalarProps.inputCols)
			sep = " "
		}
		fmt.Fprintf(&buf, "%stype=%v", sep, e.scalarProps.typ)
		buf.WriteString("]")
	}
	n := tp.Child(buf.String())
	formatExprs(n, "inputs", e.inputs())
}

func (scalarClass) initKeys(e *expr, state *queryState) {
}

func (scalarClass) updateProps(e *expr) {
	if e.scalarProps != nil {
		// For a scalar operation the required input columns is the union of the
		// input columns of its inputs.
		e.scalarProps.inputCols = bitmap{}
		for _, input := range e.inputs() {
			e.scalarProps.inputCols.UnionWith(input.scalarInputCols())
		}
	}
}

func (scalarClass) requiredProps(required *physicalProps, child int) *physicalProps {
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
	result.scalarProps = &scalarProps{
		typ: e.scalarProps.typ,
	}

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
			strings.EqualFold(def.Name, "count_rows") ||
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
