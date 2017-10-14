package v3

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

var comparisonOpMap = [...]operator{
	parser.EQ:                eqOp,
	parser.LT:                ltOp,
	parser.GT:                gtOp,
	parser.LE:                leOp,
	parser.GE:                geOp,
	parser.NE:                neOp,
	parser.In:                inOp,
	parser.NotIn:             notInOp,
	parser.Like:              likeOp,
	parser.NotLike:           notLikeOp,
	parser.ILike:             iLikeOp,
	parser.NotILike:          notILikeOp,
	parser.SimilarTo:         similarToOp,
	parser.NotSimilarTo:      notSimilarToOp,
	parser.RegMatch:          regMatchOp,
	parser.NotRegMatch:       notRegMatchOp,
	parser.RegIMatch:         regIMatchOp,
	parser.NotRegIMatch:      notRegIMatchOp,
	parser.IsDistinctFrom:    isDistinctFromOp,
	parser.IsNotDistinctFrom: isNotDistinctFromOp,
	parser.Is:                isOp,
	parser.IsNot:             isNotOp,
	parser.Any:               anyOp,
	parser.Some:              someOp,
	parser.All:               allOp,
}

var binaryOpMap = [...]operator{
	parser.Bitand:   bitandOp,
	parser.Bitor:    bitorOp,
	parser.Bitxor:   bitxorOp,
	parser.Plus:     plusOp,
	parser.Minus:    minusOp,
	parser.Mult:     multOp,
	parser.Div:      divOp,
	parser.FloorDiv: floorDivOp,
	parser.Mod:      modOp,
	parser.Pow:      powOp,
	parser.Concat:   concatOp,
	parser.LShift:   lShiftOp,
	parser.RShift:   rShiftOp,
}

var unaryOpMap = [...]operator{
	parser.UnaryPlus:       unaryPlusOp,
	parser.UnaryMinus:      unaryMinusOp,
	parser.UnaryComplement: unaryComplementOp,
}

func init() {
	scalarInfo := func(name string) operatorInfo {
		return operatorInfo{
			name: name,
			columns: func(expr *expr) []bitmapIndex {
				return nil
			},
			updateProperties: func(expr *expr) {
				// For a scalar operation the required input variables is the union of the
				// required input variables of its inputs. There are no output variables.
				expr.inputVars = 0
				expr.outputVars = 0
				for _, input := range expr.inputs() {
					expr.inputVars |= input.inputVars
				}
			},
		}
	}

	operatorTab[constOp] = scalarInfo("const")
	operatorTab[existsOp] = scalarInfo("exists")
	operatorTab[andOp] = scalarInfo("logical (AND)")
	operatorTab[orOp] = scalarInfo("logical (OR)")
	operatorTab[notOp] = scalarInfo("logical (NOT)")
	operatorTab[eqOp] = scalarInfo("comp (=)")
	operatorTab[ltOp] = scalarInfo("comp (<)")
	operatorTab[gtOp] = scalarInfo("comp (>)")
	operatorTab[leOp] = scalarInfo("comp (<=)")
	operatorTab[geOp] = scalarInfo("comp (>=)")
	operatorTab[neOp] = scalarInfo("comp (!=)")
	operatorTab[inOp] = scalarInfo("comp (IN)")
	operatorTab[notInOp] = scalarInfo("comp (NOT IN)")
	operatorTab[likeOp] = scalarInfo("comp (LIKE)")
	operatorTab[notLikeOp] = scalarInfo("comp (NOT LIKE)")
	operatorTab[iLikeOp] = scalarInfo("comp (ILIKE)")
	operatorTab[notILikeOp] = scalarInfo("comp (NOT ILIKE)")
	operatorTab[similarToOp] = scalarInfo("comp (SIMILAR TO)")
	operatorTab[notSimilarToOp] = scalarInfo("comp (NOT SIMILAR TO)")
	operatorTab[regMatchOp] = scalarInfo("comp (~)")
	operatorTab[notRegMatchOp] = scalarInfo("comp (!~)")
	operatorTab[regIMatchOp] = scalarInfo("comp (~*)")
	operatorTab[notRegIMatchOp] = scalarInfo("comp (!~*)")
	operatorTab[isDistinctFromOp] = scalarInfo("comp (IS DISTINCT FROM)")
	operatorTab[isNotDistinctFromOp] = scalarInfo("comp (IS NOT DISTINCT FROM)")
	operatorTab[isOp] = scalarInfo("comp (IS)")
	operatorTab[isNotOp] = scalarInfo("comp (IS NOT)")
	operatorTab[anyOp] = scalarInfo("comp (ANY)")
	operatorTab[someOp] = scalarInfo("comp (SOME)")
	operatorTab[allOp] = scalarInfo("comp (ALL)")
	operatorTab[bitandOp] = scalarInfo("binary (&)")
	operatorTab[bitorOp] = scalarInfo("binary (|)")
	operatorTab[bitxorOp] = scalarInfo("binary (#)")
	operatorTab[plusOp] = scalarInfo("binary (+)")
	operatorTab[minusOp] = scalarInfo("binary (-)")
	operatorTab[multOp] = scalarInfo("binary (*)")
	operatorTab[divOp] = scalarInfo("binary (/)")
	operatorTab[floorDivOp] = scalarInfo("binary (//)")
	operatorTab[modOp] = scalarInfo("binary (%)")
	operatorTab[powOp] = scalarInfo("binary (^)")
	operatorTab[concatOp] = scalarInfo("binary (||)")
	operatorTab[lShiftOp] = scalarInfo("binary (<<)")
	operatorTab[rShiftOp] = scalarInfo("binary (>>)")
	operatorTab[unaryPlusOp] = scalarInfo("unary (+)")
	operatorTab[unaryMinusOp] = scalarInfo("unary (-)")
	operatorTab[unaryComplementOp] = scalarInfo("unary (~)")
}
