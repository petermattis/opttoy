package v4

func (f *factory) constructVariable(
	col privateID,
) groupID {
	variableExpr := &variableExpr{op: variableOp, col: col}
	return f.memo.memoizeVariable(variableExpr)
}

func (f *factory) constructConst(
	value privateID,
) groupID {
	constExpr := &constExpr{op: constOp, value: value}
	return f.memo.memoizeConst(constExpr)
}

func (f *factory) constructList(
	items listID,
) groupID {
	listExpr := &listExpr{op: listOp, items: items}
	return f.memo.memoizeList(listExpr)
}

func (f *factory) constructOrderedList(
	items listID,
) groupID {
	orderedListExpr := &orderedListExpr{op: orderedListOp, items: items}
	return f.memo.memoizeOrderedList(orderedListExpr)
}

func (f *factory) constructExists(
	input groupID,
) groupID {
	existsExpr := &existsExpr{op: existsOp, input: input}
	return f.memo.memoizeExists(existsExpr)
}

func (f *factory) constructAnd(
	left groupID,
	right groupID,
) groupID {
	andExpr := &andExpr{op: andOp, left: left, right: right}
	return f.memo.memoizeAnd(andExpr)
}

func (f *factory) constructOr(
	left groupID,
	right groupID,
) groupID {
	orExpr := &orExpr{op: orOp, left: left, right: right}
	return f.memo.memoizeOr(orExpr)
}

func (f *factory) constructNot(
	input groupID,
) groupID {
	notExpr := &notExpr{op: notOp, input: input}
	return f.memo.memoizeNot(notExpr)
}

func (f *factory) constructEq(
	left groupID,
	right groupID,
) groupID {
	// [NormalizeVar]
	{
		var asVariable *variableExpr

		asVariable = f.memo.lookupNormExpr(left).asVariable()
		if asVariable == nil {
			asVariable = f.memo.lookupNormExpr(right).asVariable()
			if asVariable != nil {
				return f.constructEq(right, left)
			}
		}
	}

	// [NormalizeVarEq]
	{
		var asVariable *variableExpr

		asVariable = f.memo.lookupNormExpr(left).asVariable()
		if asVariable != nil {
			asVariable = f.memo.lookupNormExpr(right).asVariable()
			if asVariable != nil {
				if f.isLowerExpr(right, left) {
					return f.constructEq(right, left)
				}
			}
		}
	}

	eqExpr := &eqExpr{op: eqOp, left: left, right: right}
	return f.memo.memoizeEq(eqExpr)
}

func (f *factory) constructLt(
	left groupID,
	right groupID,
) groupID {
	ltExpr := &ltExpr{op: ltOp, left: left, right: right}
	return f.memo.memoizeLt(ltExpr)
}

func (f *factory) constructGt(
	left groupID,
	right groupID,
) groupID {
	gtExpr := &gtExpr{op: gtOp, left: left, right: right}
	return f.memo.memoizeGt(gtExpr)
}

func (f *factory) constructLe(
	left groupID,
	right groupID,
) groupID {
	leExpr := &leExpr{op: leOp, left: left, right: right}
	return f.memo.memoizeLe(leExpr)
}

func (f *factory) constructGe(
	left groupID,
	right groupID,
) groupID {
	geExpr := &geExpr{op: geOp, left: left, right: right}
	return f.memo.memoizeGe(geExpr)
}

func (f *factory) constructNe(
	left groupID,
	right groupID,
) groupID {
	neExpr := &neExpr{op: neOp, left: left, right: right}
	return f.memo.memoizeNe(neExpr)
}

func (f *factory) constructInOp(
	left groupID,
	right groupID,
) groupID {
	inOpExpr := &inOpExpr{op: inOpOp, left: left, right: right}
	return f.memo.memoizeInOp(inOpExpr)
}

func (f *factory) constructNotIn(
	left groupID,
	right groupID,
) groupID {
	notInExpr := &notInExpr{op: notInOp, left: left, right: right}
	return f.memo.memoizeNotIn(notInExpr)
}

func (f *factory) constructLike(
	left groupID,
	right groupID,
) groupID {
	likeExpr := &likeExpr{op: likeOp, left: left, right: right}
	return f.memo.memoizeLike(likeExpr)
}

func (f *factory) constructNotLike(
	left groupID,
	right groupID,
) groupID {
	notLikeExpr := &notLikeExpr{op: notLikeOp, left: left, right: right}
	return f.memo.memoizeNotLike(notLikeExpr)
}

func (f *factory) constructILike(
	left groupID,
	right groupID,
) groupID {
	iLikeExpr := &iLikeExpr{op: iLikeOp, left: left, right: right}
	return f.memo.memoizeILike(iLikeExpr)
}

func (f *factory) constructNotILike(
	left groupID,
	right groupID,
) groupID {
	notILikeExpr := &notILikeExpr{op: notILikeOp, left: left, right: right}
	return f.memo.memoizeNotILike(notILikeExpr)
}

func (f *factory) constructSimilarTo(
	left groupID,
	right groupID,
) groupID {
	similarToExpr := &similarToExpr{op: similarToOp, left: left, right: right}
	return f.memo.memoizeSimilarTo(similarToExpr)
}

func (f *factory) constructNotSimilarTo(
	left groupID,
	right groupID,
) groupID {
	notSimilarToExpr := &notSimilarToExpr{op: notSimilarToOp, left: left, right: right}
	return f.memo.memoizeNotSimilarTo(notSimilarToExpr)
}

func (f *factory) constructRegMatch(
	left groupID,
	right groupID,
) groupID {
	regMatchExpr := &regMatchExpr{op: regMatchOp, left: left, right: right}
	return f.memo.memoizeRegMatch(regMatchExpr)
}

func (f *factory) constructNotRegMatch(
	left groupID,
	right groupID,
) groupID {
	notRegMatchExpr := &notRegMatchExpr{op: notRegMatchOp, left: left, right: right}
	return f.memo.memoizeNotRegMatch(notRegMatchExpr)
}

func (f *factory) constructRegIMatch(
	left groupID,
	right groupID,
) groupID {
	regIMatchExpr := &regIMatchExpr{op: regIMatchOp, left: left, right: right}
	return f.memo.memoizeRegIMatch(regIMatchExpr)
}

func (f *factory) constructNotRegIMatch(
	left groupID,
	right groupID,
) groupID {
	notRegIMatchExpr := &notRegIMatchExpr{op: notRegIMatchOp, left: left, right: right}
	return f.memo.memoizeNotRegIMatch(notRegIMatchExpr)
}

func (f *factory) constructIsDistinctFrom(
	left groupID,
	right groupID,
) groupID {
	isDistinctFromExpr := &isDistinctFromExpr{op: isDistinctFromOp, left: left, right: right}
	return f.memo.memoizeIsDistinctFrom(isDistinctFromExpr)
}

func (f *factory) constructIsNotDistinctFrom(
	left groupID,
	right groupID,
) groupID {
	isNotDistinctFromExpr := &isNotDistinctFromExpr{op: isNotDistinctFromOp, left: left, right: right}
	return f.memo.memoizeIsNotDistinctFrom(isNotDistinctFromExpr)
}

func (f *factory) constructIs(
	left groupID,
	right groupID,
) groupID {
	isExpr := &isExpr{op: isOp, left: left, right: right}
	return f.memo.memoizeIs(isExpr)
}

func (f *factory) constructIsNot(
	left groupID,
	right groupID,
) groupID {
	isNotExpr := &isNotExpr{op: isNotOp, left: left, right: right}
	return f.memo.memoizeIsNot(isNotExpr)
}

func (f *factory) constructAny(
	left groupID,
	right groupID,
) groupID {
	anyExpr := &anyExpr{op: anyOp, left: left, right: right}
	return f.memo.memoizeAny(anyExpr)
}

func (f *factory) constructSome(
	left groupID,
	right groupID,
) groupID {
	someExpr := &someExpr{op: someOp, left: left, right: right}
	return f.memo.memoizeSome(someExpr)
}

func (f *factory) constructAll(
	left groupID,
	right groupID,
) groupID {
	allExpr := &allExpr{op: allOp, left: left, right: right}
	return f.memo.memoizeAll(allExpr)
}

func (f *factory) constructBitAnd(
	left groupID,
	right groupID,
) groupID {
	bitAndExpr := &bitAndExpr{op: bitAndOp, left: left, right: right}
	return f.memo.memoizeBitAnd(bitAndExpr)
}

func (f *factory) constructBitOr(
	left groupID,
	right groupID,
) groupID {
	bitOrExpr := &bitOrExpr{op: bitOrOp, left: left, right: right}
	return f.memo.memoizeBitOr(bitOrExpr)
}

func (f *factory) constructBitXor(
	left groupID,
	right groupID,
) groupID {
	bitXorExpr := &bitXorExpr{op: bitXorOp, left: left, right: right}
	return f.memo.memoizeBitXor(bitXorExpr)
}

func (f *factory) constructPlus(
	left groupID,
	right groupID,
) groupID {
	plusExpr := &plusExpr{op: plusOp, left: left, right: right}
	return f.memo.memoizePlus(plusExpr)
}

func (f *factory) constructMinus(
	left groupID,
	right groupID,
) groupID {
	minusExpr := &minusExpr{op: minusOp, left: left, right: right}
	return f.memo.memoizeMinus(minusExpr)
}

func (f *factory) constructMult(
	left groupID,
	right groupID,
) groupID {
	multExpr := &multExpr{op: multOp, left: left, right: right}
	return f.memo.memoizeMult(multExpr)
}

func (f *factory) constructDiv(
	left groupID,
	right groupID,
) groupID {
	divExpr := &divExpr{op: divOp, left: left, right: right}
	return f.memo.memoizeDiv(divExpr)
}

func (f *factory) constructFloorDiv(
	left groupID,
	right groupID,
) groupID {
	floorDivExpr := &floorDivExpr{op: floorDivOp, left: left, right: right}
	return f.memo.memoizeFloorDiv(floorDivExpr)
}

func (f *factory) constructMod(
	left groupID,
	right groupID,
) groupID {
	modExpr := &modExpr{op: modOp, left: left, right: right}
	return f.memo.memoizeMod(modExpr)
}

func (f *factory) constructPow(
	left groupID,
	right groupID,
) groupID {
	powExpr := &powExpr{op: powOp, left: left, right: right}
	return f.memo.memoizePow(powExpr)
}

func (f *factory) constructConcat(
	left groupID,
	right groupID,
) groupID {
	concatExpr := &concatExpr{op: concatOp, left: left, right: right}
	return f.memo.memoizeConcat(concatExpr)
}

func (f *factory) constructLShift(
	left groupID,
	right groupID,
) groupID {
	lShiftExpr := &lShiftExpr{op: lShiftOp, left: left, right: right}
	return f.memo.memoizeLShift(lShiftExpr)
}

func (f *factory) constructRShift(
	left groupID,
	right groupID,
) groupID {
	rShiftExpr := &rShiftExpr{op: rShiftOp, left: left, right: right}
	return f.memo.memoizeRShift(rShiftExpr)
}

func (f *factory) constructUnaryPlus(
	input groupID,
) groupID {
	unaryPlusExpr := &unaryPlusExpr{op: unaryPlusOp, input: input}
	return f.memo.memoizeUnaryPlus(unaryPlusExpr)
}

func (f *factory) constructUnaryMinus(
	input groupID,
) groupID {
	unaryMinusExpr := &unaryMinusExpr{op: unaryMinusOp, input: input}
	return f.memo.memoizeUnaryMinus(unaryMinusExpr)
}

func (f *factory) constructUnaryComplement(
	input groupID,
) groupID {
	unaryComplementExpr := &unaryComplementExpr{op: unaryComplementOp, input: input}
	return f.memo.memoizeUnaryComplement(unaryComplementExpr)
}

func (f *factory) constructFunction(
	args listID,
	def privateID,
) groupID {
	functionExpr := &functionExpr{op: functionOp, args: args, def: def}
	return f.memo.memoizeFunction(functionExpr)
}
