package v3

import (
	"bytes"
	"fmt"
)

const spaces = "                                                                "

func init() {
	registerOperator(constOp, "const", scalar{})
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
}

type scalar struct{}

func (scalar) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v", indent, e.op)
	if e.props != nil {
		if data := e.props.state.getData(e.dataIndex); data != nil {
			fmt.Fprintf(buf, " (%s)", data)
		}
	}
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (scalar) updateProperties(e *expr) {
	// For a scalar operation the required input variables is the union of the
	// required input variables of its inputs. There are no output variables.
	e.inputVars = 0
	e.outputVars = 0
	for _, input := range e.inputs() {
		e.inputVars |= input.inputVars
	}
}
