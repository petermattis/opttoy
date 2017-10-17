package v3

import (
	"bytes"
	"fmt"
)

const spaces = "                                                                "

func init() {
	scalarFormat := func(e *expr, buf *bytes.Buffer, level int) {
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

	scalarUpdateProperties := func(expr *expr) {
		// For a scalar operation the required input variables is the union of the
		// required input variables of its inputs. There are no output variables.
		expr.inputVars = 0
		expr.outputVars = 0
		for _, input := range expr.inputs() {
			expr.inputVars |= input.inputVars
		}
	}

	scalarInfo := func() operatorInfo {
		return operatorInfo{
			format:           scalarFormat,
			updateProperties: scalarUpdateProperties,
		}
	}

	registerOperator(constOp, "const", scalarInfo())
	registerOperator(existsOp, "exists", scalarInfo())
	registerOperator(andOp, "logical (AND)", scalarInfo())
	registerOperator(orOp, "logical (OR)", scalarInfo())
	registerOperator(notOp, "logical (NOT)", scalarInfo())
	registerOperator(eqOp, "comp (=)", scalarInfo())
	registerOperator(ltOp, "comp (<)", scalarInfo())
	registerOperator(gtOp, "comp (>)", scalarInfo())
	registerOperator(leOp, "comp (<=)", scalarInfo())
	registerOperator(geOp, "comp (>=)", scalarInfo())
	registerOperator(neOp, "comp (!=)", scalarInfo())
	registerOperator(inOp, "comp (IN)", scalarInfo())
	registerOperator(notInOp, "comp (NOT IN)", scalarInfo())
	registerOperator(likeOp, "comp (LIKE)", scalarInfo())
	registerOperator(notLikeOp, "comp (NOT LIKE)", scalarInfo())
	registerOperator(iLikeOp, "comp (ILIKE)", scalarInfo())
	registerOperator(notILikeOp, "comp (NOT ILIKE)", scalarInfo())
	registerOperator(similarToOp, "comp (SIMILAR TO)", scalarInfo())
	registerOperator(notSimilarToOp, "comp (NOT SIMILAR TO)", scalarInfo())
	registerOperator(regMatchOp, "comp (~)", scalarInfo())
	registerOperator(notRegMatchOp, "comp (!~)", scalarInfo())
	registerOperator(regIMatchOp, "comp (~*)", scalarInfo())
	registerOperator(notRegIMatchOp, "comp (!~*)", scalarInfo())
	registerOperator(isDistinctFromOp, "comp (IS DISTINCT FROM)", scalarInfo())
	registerOperator(isNotDistinctFromOp, "comp (IS NOT DISTINCT FROM)", scalarInfo())
	registerOperator(isOp, "comp (IS)", scalarInfo())
	registerOperator(isNotOp, "comp (IS NOT)", scalarInfo())
	registerOperator(anyOp, "comp (ANY)", scalarInfo())
	registerOperator(someOp, "comp (SOME)", scalarInfo())
	registerOperator(allOp, "comp (ALL)", scalarInfo())
	registerOperator(bitandOp, "binary (&)", scalarInfo())
	registerOperator(bitorOp, "binary (|)", scalarInfo())
	registerOperator(bitxorOp, "binary (#)", scalarInfo())
	registerOperator(plusOp, "binary (+)", scalarInfo())
	registerOperator(minusOp, "binary (-)", scalarInfo())
	registerOperator(multOp, "binary (*)", scalarInfo())
	registerOperator(divOp, "binary (/)", scalarInfo())
	registerOperator(floorDivOp, "binary (//)", scalarInfo())
	registerOperator(modOp, "binary (%)", scalarInfo())
	registerOperator(powOp, "binary (^)", scalarInfo())
	registerOperator(concatOp, "binary (||)", scalarInfo())
	registerOperator(lShiftOp, "binary (<<)", scalarInfo())
	registerOperator(rShiftOp, "binary (>>)", scalarInfo())
	registerOperator(unaryPlusOp, "unary (+)", scalarInfo())
	registerOperator(unaryMinusOp, "unary (-)", scalarInfo())
	registerOperator(unaryComplementOp, "unary (~)", scalarInfo())
}
