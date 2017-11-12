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
	registerOperator(existsOp, "exists", scalar{})
	registerOperator(andOp, "logical (AND)", scalar{})
	registerOperator(orOp, "logical (OR)", scalar{})
	registerOperator(notOp, "logical (NOT)", scalar{})
	registerOperator(eqOp, "comp (=)", scalar{})
	registerOperator(ltOp, "comp (<)", scalar{})
	registerOperator(gtOp, "comp (>)", scalar{})
	registerOperator(leOp, "comp (<=)", scalar{})
	registerOperator(geOp, "comp (>=)", scalar{})
	registerOperator(neOp, "comp (!=)", scalar{})
	registerOperator(inOp, "comp (IN)", scalar{})
	registerOperator(notInOp, "comp (NOT IN)", scalar{})
	registerOperator(likeOp, "comp (LIKE)", scalar{})
	registerOperator(notLikeOp, "comp (NOT LIKE)", scalar{})
	registerOperator(iLikeOp, "comp (ILIKE)", scalar{})
	registerOperator(notILikeOp, "comp (NOT ILIKE)", scalar{})
	registerOperator(similarToOp, "comp (SIMILAR TO)", scalar{})
	registerOperator(notSimilarToOp, "comp (NOT SIMILAR TO)", scalar{})
	registerOperator(regMatchOp, "comp (~)", scalar{})
	registerOperator(notRegMatchOp, "comp (!~)", scalar{})
	registerOperator(regIMatchOp, "comp (~*)", scalar{})
	registerOperator(notRegIMatchOp, "comp (!~*)", scalar{})
	registerOperator(isDistinctFromOp, "comp (IS DISTINCT FROM)", scalar{})
	registerOperator(isNotDistinctFromOp, "comp (IS NOT DISTINCT FROM)", scalar{})
	registerOperator(isOp, "comp (IS)", scalar{})
	registerOperator(isNotOp, "comp (IS NOT)", scalar{})
	registerOperator(anyOp, "comp (ANY)", scalar{})
	registerOperator(someOp, "comp (SOME)", scalar{})
	registerOperator(allOp, "comp (ALL)", scalar{})
	registerOperator(bitandOp, "binary (&)", scalar{})
	registerOperator(bitorOp, "binary (|)", scalar{})
	registerOperator(bitxorOp, "binary (#)", scalar{})
	registerOperator(plusOp, "binary (+)", scalar{})
	registerOperator(minusOp, "binary (-)", scalar{})
	registerOperator(multOp, "binary (*)", scalar{})
	registerOperator(divOp, "binary (/)", scalar{})
	registerOperator(floorDivOp, "binary (//)", scalar{})
	registerOperator(modOp, "binary (%)", scalar{})
	registerOperator(powOp, "binary (^)", scalar{})
	registerOperator(concatOp, "binary (||)", scalar{})
	registerOperator(lShiftOp, "binary (<<)", scalar{})
	registerOperator(rShiftOp, "binary (>>)", scalar{})
	registerOperator(unaryPlusOp, "unary (+)", scalar{})
	registerOperator(unaryMinusOp, "unary (-)", scalar{})
	registerOperator(unaryComplementOp, "unary (~)", scalar{})
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
		extra:       0,
		children:    []*expr{input1},
		scalarProps: &scalarProps{},
	}
	e.updateProps()
	return e
}

func newBinaryExpr(op operator, input1, input2 *expr) *expr {
	e := &expr{
		op:          op,
		extra:       0,
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

func (scalar) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v", indent, e.op)
	if e.private != nil {
		fmt.Fprintf(buf, " (%s)", e.private)
	}
	if e.scalarProps != nil && e.scalarProps.inputVars != 0 {
		fmt.Fprintf(buf, " [in=%s]", e.scalarProps.inputVars)
	}
	buf.WriteString("\n")
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (scalar) initKeys(e *expr, state *queryState) {
}

func (s scalar) updateProps(e *expr) {
	if e.scalarProps != nil {
		// For a scalar operation the required input variables is the union of the
		// required input variables of its inputs.
		e.scalarProps.inputVars = 0
		for _, input := range e.inputs() {
			e.scalarProps.inputVars.unionWith(input.scalarInputVars())
		}
	}
}

func (scalar) requiredInputVars(e *expr) bitmap {
	return e.providedInputVars()
}

func substitute(e *expr, columns bitmap, replacement *expr) *expr {
	if e.op == variableOp && e.scalarInputVars() == columns {
		return replacement
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
