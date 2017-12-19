package v4

func (_f *factory) constructVariable(
	col privateID,
) groupID {
	_variable := variableExpr{op: variableOp, col: col}
	_fingerprint := _variable.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeVariable(&_variable)
}

func (_f *factory) constructConst(
	value privateID,
) groupID {
	_const := constExpr{op: constOp, value: value}
	_fingerprint := _const.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeConst(&_const)
}

func (_f *factory) constructList(
	items listID,
) groupID {
	_list := listExpr{op: listOp, items: items}
	_fingerprint := _list.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeList(&_list)
}

func (_f *factory) constructOrderedList(
	items listID,
) groupID {
	_orderedList := orderedListExpr{op: orderedListOp, items: items}
	_fingerprint := _orderedList.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeOrderedList(&_orderedList)
}

func (_f *factory) constructExists(
	input groupID,
) groupID {
	_exists := existsExpr{op: existsOp, input: input}
	_fingerprint := _exists.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeExists(&_exists)
}

func (_f *factory) constructAnd(
	left groupID,
	right groupID,
) groupID {
	_and := andExpr{op: andOp, left: left, right: right}
	_fingerprint := _and.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeAnd(&_and)
}

func (_f *factory) constructOr(
	left groupID,
	right groupID,
) groupID {
	_or := orExpr{op: orOp, left: left, right: right}
	_fingerprint := _or.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeOr(&_or)
}

func (_f *factory) constructNot(
	input groupID,
) groupID {
	_not := notExpr{op: notOp, input: input}
	_fingerprint := _not.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNot(&_not)
}

func (_f *factory) constructEq(
	left groupID,
	right groupID,
) groupID {
	_eq := eqExpr{op: eqOp, left: left, right: right}
	_fingerprint := _eq.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	// [NormalizeVar]
	{
		_variable := _f.memo.lookupNormExpr(left).asVariable()
		if _variable == nil {
			_variable2 := _f.memo.lookupNormExpr(right).asVariable()
			if _variable2 != nil {
				_group = _f.constructEq(right, left)
				_f.memo.addAltFingerprint(_fingerprint, _group)
				return _group
			}
		}
	}

	// [NormalizeVarEq]
	{
		_variable := _f.memo.lookupNormExpr(left).asVariable()
		if _variable != nil {
			_variable2 := _f.memo.lookupNormExpr(right).asVariable()
			if _variable2 != nil {
				if _f.isLowerExpr(right, left) {
					_group = _f.constructEq(right, left)
					_f.memo.addAltFingerprint(_fingerprint, _group)
					return _group
				}
			}
		}
	}

	return _f.memo.memoizeEq(&_eq)
}

func (_f *factory) constructLt(
	left groupID,
	right groupID,
) groupID {
	_lt := ltExpr{op: ltOp, left: left, right: right}
	_fingerprint := _lt.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeLt(&_lt)
}

func (_f *factory) constructGt(
	left groupID,
	right groupID,
) groupID {
	_gt := gtExpr{op: gtOp, left: left, right: right}
	_fingerprint := _gt.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeGt(&_gt)
}

func (_f *factory) constructLe(
	left groupID,
	right groupID,
) groupID {
	_le := leExpr{op: leOp, left: left, right: right}
	_fingerprint := _le.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeLe(&_le)
}

func (_f *factory) constructGe(
	left groupID,
	right groupID,
) groupID {
	_ge := geExpr{op: geOp, left: left, right: right}
	_fingerprint := _ge.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeGe(&_ge)
}

func (_f *factory) constructNe(
	left groupID,
	right groupID,
) groupID {
	_ne := neExpr{op: neOp, left: left, right: right}
	_fingerprint := _ne.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNe(&_ne)
}

func (_f *factory) constructIn(
	left groupID,
	right groupID,
) groupID {
	_in := inExpr{op: inOp, left: left, right: right}
	_fingerprint := _in.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeIn(&_in)
}

func (_f *factory) constructNotIn(
	left groupID,
	right groupID,
) groupID {
	_notIn := notInExpr{op: notInOp, left: left, right: right}
	_fingerprint := _notIn.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNotIn(&_notIn)
}

func (_f *factory) constructLike(
	left groupID,
	right groupID,
) groupID {
	_like := likeExpr{op: likeOp, left: left, right: right}
	_fingerprint := _like.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeLike(&_like)
}

func (_f *factory) constructNotLike(
	left groupID,
	right groupID,
) groupID {
	_notLike := notLikeExpr{op: notLikeOp, left: left, right: right}
	_fingerprint := _notLike.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNotLike(&_notLike)
}

func (_f *factory) constructILike(
	left groupID,
	right groupID,
) groupID {
	_iLike := iLikeExpr{op: iLikeOp, left: left, right: right}
	_fingerprint := _iLike.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeILike(&_iLike)
}

func (_f *factory) constructNotILike(
	left groupID,
	right groupID,
) groupID {
	_notILike := notILikeExpr{op: notILikeOp, left: left, right: right}
	_fingerprint := _notILike.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNotILike(&_notILike)
}

func (_f *factory) constructSimilarTo(
	left groupID,
	right groupID,
) groupID {
	_similarTo := similarToExpr{op: similarToOp, left: left, right: right}
	_fingerprint := _similarTo.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeSimilarTo(&_similarTo)
}

func (_f *factory) constructNotSimilarTo(
	left groupID,
	right groupID,
) groupID {
	_notSimilarTo := notSimilarToExpr{op: notSimilarToOp, left: left, right: right}
	_fingerprint := _notSimilarTo.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNotSimilarTo(&_notSimilarTo)
}

func (_f *factory) constructRegMatch(
	left groupID,
	right groupID,
) groupID {
	_regMatch := regMatchExpr{op: regMatchOp, left: left, right: right}
	_fingerprint := _regMatch.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeRegMatch(&_regMatch)
}

func (_f *factory) constructNotRegMatch(
	left groupID,
	right groupID,
) groupID {
	_notRegMatch := notRegMatchExpr{op: notRegMatchOp, left: left, right: right}
	_fingerprint := _notRegMatch.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNotRegMatch(&_notRegMatch)
}

func (_f *factory) constructRegIMatch(
	left groupID,
	right groupID,
) groupID {
	_regIMatch := regIMatchExpr{op: regIMatchOp, left: left, right: right}
	_fingerprint := _regIMatch.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeRegIMatch(&_regIMatch)
}

func (_f *factory) constructNotRegIMatch(
	left groupID,
	right groupID,
) groupID {
	_notRegIMatch := notRegIMatchExpr{op: notRegIMatchOp, left: left, right: right}
	_fingerprint := _notRegIMatch.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeNotRegIMatch(&_notRegIMatch)
}

func (_f *factory) constructIsDistinctFrom(
	left groupID,
	right groupID,
) groupID {
	_isDistinctFrom := isDistinctFromExpr{op: isDistinctFromOp, left: left, right: right}
	_fingerprint := _isDistinctFrom.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeIsDistinctFrom(&_isDistinctFrom)
}

func (_f *factory) constructIsNotDistinctFrom(
	left groupID,
	right groupID,
) groupID {
	_isNotDistinctFrom := isNotDistinctFromExpr{op: isNotDistinctFromOp, left: left, right: right}
	_fingerprint := _isNotDistinctFrom.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeIsNotDistinctFrom(&_isNotDistinctFrom)
}

func (_f *factory) constructIs(
	left groupID,
	right groupID,
) groupID {
	_is := isExpr{op: isOp, left: left, right: right}
	_fingerprint := _is.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeIs(&_is)
}

func (_f *factory) constructIsNot(
	left groupID,
	right groupID,
) groupID {
	_isNot := isNotExpr{op: isNotOp, left: left, right: right}
	_fingerprint := _isNot.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeIsNot(&_isNot)
}

func (_f *factory) constructAny(
	left groupID,
	right groupID,
) groupID {
	_any := anyExpr{op: anyOp, left: left, right: right}
	_fingerprint := _any.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeAny(&_any)
}

func (_f *factory) constructSome(
	left groupID,
	right groupID,
) groupID {
	_some := someExpr{op: someOp, left: left, right: right}
	_fingerprint := _some.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeSome(&_some)
}

func (_f *factory) constructAll(
	left groupID,
	right groupID,
) groupID {
	_all := allExpr{op: allOp, left: left, right: right}
	_fingerprint := _all.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeAll(&_all)
}

func (_f *factory) constructBitand(
	left groupID,
	right groupID,
) groupID {
	_bitand := bitandExpr{op: bitandOp, left: left, right: right}
	_fingerprint := _bitand.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeBitand(&_bitand)
}

func (_f *factory) constructBitor(
	left groupID,
	right groupID,
) groupID {
	_bitor := bitorExpr{op: bitorOp, left: left, right: right}
	_fingerprint := _bitor.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeBitor(&_bitor)
}

func (_f *factory) constructBitxor(
	left groupID,
	right groupID,
) groupID {
	_bitxor := bitxorExpr{op: bitxorOp, left: left, right: right}
	_fingerprint := _bitxor.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeBitxor(&_bitxor)
}

func (_f *factory) constructPlus(
	left groupID,
	right groupID,
) groupID {
	_plus := plusExpr{op: plusOp, left: left, right: right}
	_fingerprint := _plus.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizePlus(&_plus)
}

func (_f *factory) constructMinus(
	left groupID,
	right groupID,
) groupID {
	_minus := minusExpr{op: minusOp, left: left, right: right}
	_fingerprint := _minus.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeMinus(&_minus)
}

func (_f *factory) constructMult(
	left groupID,
	right groupID,
) groupID {
	_mult := multExpr{op: multOp, left: left, right: right}
	_fingerprint := _mult.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeMult(&_mult)
}

func (_f *factory) constructDiv(
	left groupID,
	right groupID,
) groupID {
	_div := divExpr{op: divOp, left: left, right: right}
	_fingerprint := _div.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeDiv(&_div)
}

func (_f *factory) constructFloorDiv(
	left groupID,
	right groupID,
) groupID {
	_floorDiv := floorDivExpr{op: floorDivOp, left: left, right: right}
	_fingerprint := _floorDiv.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeFloorDiv(&_floorDiv)
}

func (_f *factory) constructMod(
	left groupID,
	right groupID,
) groupID {
	_mod := modExpr{op: modOp, left: left, right: right}
	_fingerprint := _mod.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeMod(&_mod)
}

func (_f *factory) constructPow(
	left groupID,
	right groupID,
) groupID {
	_pow := powExpr{op: powOp, left: left, right: right}
	_fingerprint := _pow.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizePow(&_pow)
}

func (_f *factory) constructConcat(
	left groupID,
	right groupID,
) groupID {
	_concat := concatExpr{op: concatOp, left: left, right: right}
	_fingerprint := _concat.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeConcat(&_concat)
}

func (_f *factory) constructLShift(
	left groupID,
	right groupID,
) groupID {
	_lShift := lShiftExpr{op: lShiftOp, left: left, right: right}
	_fingerprint := _lShift.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeLShift(&_lShift)
}

func (_f *factory) constructRShift(
	left groupID,
	right groupID,
) groupID {
	_rShift := rShiftExpr{op: rShiftOp, left: left, right: right}
	_fingerprint := _rShift.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeRShift(&_rShift)
}

func (_f *factory) constructUnaryPlus(
	input groupID,
) groupID {
	_unaryPlus := unaryPlusExpr{op: unaryPlusOp, input: input}
	_fingerprint := _unaryPlus.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeUnaryPlus(&_unaryPlus)
}

func (_f *factory) constructUnaryMinus(
	input groupID,
) groupID {
	_unaryMinus := unaryMinusExpr{op: unaryMinusOp, input: input}
	_fingerprint := _unaryMinus.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeUnaryMinus(&_unaryMinus)
}

func (_f *factory) constructUnaryComplement(
	input groupID,
) groupID {
	_unaryComplement := unaryComplementExpr{op: unaryComplementOp, input: input}
	_fingerprint := _unaryComplement.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeUnaryComplement(&_unaryComplement)
}

func (_f *factory) constructFunction(
	args listID,
	def privateID,
) groupID {
	_function := functionExpr{op: functionOp, args: args, def: def}
	_fingerprint := _function.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeFunction(&_function)
}

func (_f *factory) constructScan(
	table privateID,
) groupID {
	_scan := scanExpr{op: scanOp, table: table}
	_fingerprint := _scan.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeScan(&_scan)
}

func (_f *factory) constructSelect(
	input groupID,
	filter groupID,
) groupID {
	_select := selectExpr{op: selectOp, input: input, filter: filter}
	_fingerprint := _select.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeSelect(&_select)
}

func (_f *factory) constructInnerJoin(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_innerJoin := innerJoinExpr{op: innerJoinOp, left: left, right: right, filter: filter}
	_fingerprint := _innerJoin.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeInnerJoin(&_innerJoin)
}

func (_f *factory) constructLeftJoin(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_leftJoin := leftJoinExpr{op: leftJoinOp, left: left, right: right, filter: filter}
	_fingerprint := _leftJoin.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeLeftJoin(&_leftJoin)
}

func (_f *factory) constructRightJoin(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_rightJoin := rightJoinExpr{op: rightJoinOp, left: left, right: right, filter: filter}
	_fingerprint := _rightJoin.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeRightJoin(&_rightJoin)
}

func (_f *factory) constructFullJoin(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_fullJoin := fullJoinExpr{op: fullJoinOp, left: left, right: right, filter: filter}
	_fingerprint := _fullJoin.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeFullJoin(&_fullJoin)
}

func (_f *factory) constructSemiJoin(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_semiJoin := semiJoinExpr{op: semiJoinOp, left: left, right: right, filter: filter}
	_fingerprint := _semiJoin.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeSemiJoin(&_semiJoin)
}

func (_f *factory) constructAntiJoin(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_antiJoin := antiJoinExpr{op: antiJoinOp, left: left, right: right, filter: filter}
	_fingerprint := _antiJoin.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeAntiJoin(&_antiJoin)
}

func (_f *factory) constructInnerJoinApply(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_innerJoinApply := innerJoinApplyExpr{op: innerJoinApplyOp, left: left, right: right, filter: filter}
	_fingerprint := _innerJoinApply.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeInnerJoinApply(&_innerJoinApply)
}

func (_f *factory) constructLeftJoinApply(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_leftJoinApply := leftJoinApplyExpr{op: leftJoinApplyOp, left: left, right: right, filter: filter}
	_fingerprint := _leftJoinApply.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeLeftJoinApply(&_leftJoinApply)
}

func (_f *factory) constructRightJoinApply(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_rightJoinApply := rightJoinApplyExpr{op: rightJoinApplyOp, left: left, right: right, filter: filter}
	_fingerprint := _rightJoinApply.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeRightJoinApply(&_rightJoinApply)
}

func (_f *factory) constructFullJoinApply(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_fullJoinApply := fullJoinApplyExpr{op: fullJoinApplyOp, left: left, right: right, filter: filter}
	_fingerprint := _fullJoinApply.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeFullJoinApply(&_fullJoinApply)
}

func (_f *factory) constructSemiJoinApply(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_semiJoinApply := semiJoinApplyExpr{op: semiJoinApplyOp, left: left, right: right, filter: filter}
	_fingerprint := _semiJoinApply.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeSemiJoinApply(&_semiJoinApply)
}

func (_f *factory) constructAntiJoinApply(
	left groupID,
	right groupID,
	filter groupID,
) groupID {
	_antiJoinApply := antiJoinApplyExpr{op: antiJoinApplyOp, left: left, right: right, filter: filter}
	_fingerprint := _antiJoinApply.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeAntiJoinApply(&_antiJoinApply)
}

func (_f *factory) constructSort(
	input groupID,
	orderSpec privateID,
) groupID {
	_sort := sortExpr{op: sortOp, input: input, orderSpec: orderSpec}
	_fingerprint := _sort.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeSort(&_sort)
}

func (_f *factory) constructProjectSubset(
	input groupID,
	projections groupID,
) groupID {
	_projectSubset := projectSubsetExpr{op: projectSubsetOp, input: input, projections: projections}
	_fingerprint := _projectSubset.fingerprint()
	_group := _f.memo.lookupGroupByFingerprint(_fingerprint)
	if _group != 0 {
		return _group
	}

	return _f.memo.memoizeProjectSubset(&_projectSubset)
}
