package v3

func init() {
	scalarColumns := func(expr *expr) []bitmapIndex {
		return nil
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

	scalarInfo := func(name string) operatorInfo {
		return operatorInfo{
			name:             name,
			columns:          scalarColumns,
			updateProperties: scalarUpdateProperties,
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
