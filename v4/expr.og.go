package v4

import (
	"crypto/md5"
	"unsafe"
)

type childCountLookupFunc func(e *expr) int

var childCountLookup = []childCountLookupFunc{
	nil,

	// variableOp
	func(e *expr) int {
		return 0
	},

	// constOp
	func(e *expr) int {
		return 0
	},

	// listOp
	func(e *expr) int {
		listExpr := (*listExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))
		return 0 + int(listExpr.items.len)
	},

	// orderedListOp
	func(e *expr) int {
		orderedListExpr := (*orderedListExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))
		return 0 + int(orderedListExpr.items.len)
	},

	// existsOp
	func(e *expr) int {
		return 1
	},

	// andOp
	func(e *expr) int {
		return 2
	},

	// orOp
	func(e *expr) int {
		return 2
	},

	// notOp
	func(e *expr) int {
		return 1
	},

	// eqOp
	func(e *expr) int {
		return 2
	},

	// ltOp
	func(e *expr) int {
		return 2
	},

	// gtOp
	func(e *expr) int {
		return 2
	},

	// leOp
	func(e *expr) int {
		return 2
	},

	// geOp
	func(e *expr) int {
		return 2
	},

	// neOp
	func(e *expr) int {
		return 2
	},

	// inOp
	func(e *expr) int {
		return 2
	},

	// notInOp
	func(e *expr) int {
		return 2
	},

	// likeOp
	func(e *expr) int {
		return 2
	},

	// notLikeOp
	func(e *expr) int {
		return 2
	},

	// iLikeOp
	func(e *expr) int {
		return 2
	},

	// notILikeOp
	func(e *expr) int {
		return 2
	},

	// similarToOp
	func(e *expr) int {
		return 2
	},

	// notSimilarToOp
	func(e *expr) int {
		return 2
	},

	// regMatchOp
	func(e *expr) int {
		return 2
	},

	// notRegMatchOp
	func(e *expr) int {
		return 2
	},

	// regIMatchOp
	func(e *expr) int {
		return 2
	},

	// notRegIMatchOp
	func(e *expr) int {
		return 2
	},

	// isDistinctFromOp
	func(e *expr) int {
		return 2
	},

	// isNotDistinctFromOp
	func(e *expr) int {
		return 2
	},

	// isOp
	func(e *expr) int {
		return 2
	},

	// isNotOp
	func(e *expr) int {
		return 2
	},

	// anyOp
	func(e *expr) int {
		return 2
	},

	// someOp
	func(e *expr) int {
		return 2
	},

	// allOp
	func(e *expr) int {
		return 2
	},

	// bitandOp
	func(e *expr) int {
		return 2
	},

	// bitorOp
	func(e *expr) int {
		return 2
	},

	// bitxorOp
	func(e *expr) int {
		return 2
	},

	// plusOp
	func(e *expr) int {
		return 2
	},

	// minusOp
	func(e *expr) int {
		return 2
	},

	// multOp
	func(e *expr) int {
		return 2
	},

	// divOp
	func(e *expr) int {
		return 2
	},

	// floorDivOp
	func(e *expr) int {
		return 2
	},

	// modOp
	func(e *expr) int {
		return 2
	},

	// powOp
	func(e *expr) int {
		return 2
	},

	// concatOp
	func(e *expr) int {
		return 2
	},

	// lShiftOp
	func(e *expr) int {
		return 2
	},

	// rShiftOp
	func(e *expr) int {
		return 2
	},

	// unaryPlusOp
	func(e *expr) int {
		return 1
	},

	// unaryMinusOp
	func(e *expr) int {
		return 1
	},

	// unaryComplementOp
	func(e *expr) int {
		return 1
	},

	// functionOp
	func(e *expr) int {
		functionExpr := (*functionExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))
		return 0 + int(functionExpr.args.len)
	},

	// scanOp
	func(e *expr) int {
		return 0
	},

	// selectOp
	func(e *expr) int {
		return 2
	},

	// innerJoinOp
	func(e *expr) int {
		return 3
	},

	// leftJoinOp
	func(e *expr) int {
		return 3
	},

	// rightJoinOp
	func(e *expr) int {
		return 3
	},

	// fullJoinOp
	func(e *expr) int {
		return 3
	},

	// semiJoinOp
	func(e *expr) int {
		return 3
	},

	// antiJoinOp
	func(e *expr) int {
		return 3
	},

	// innerJoinApplyOp
	func(e *expr) int {
		return 3
	},

	// leftJoinApplyOp
	func(e *expr) int {
		return 3
	},

	// rightJoinApplyOp
	func(e *expr) int {
		return 3
	},

	// fullJoinApplyOp
	func(e *expr) int {
		return 3
	},

	// semiJoinApplyOp
	func(e *expr) int {
		return 3
	},

	// antiJoinApplyOp
	func(e *expr) int {
		return 3
	},

	// sortOp
	func(e *expr) int {
		return 1
	},

	// projectSubsetOp
	func(e *expr) int {
		return 2
	},
}

type childGroupLookupFunc func(e *expr, n int) groupID

var childGroupLookup = []childGroupLookupFunc{
	nil, // unknownOp

	// variableOp
	func(e *expr, n int) groupID {
		panic("child index out of range")
	},

	// constOp
	func(e *expr, n int) groupID {
		panic("child index out of range")
	},

	// listOp
	func(e *expr, n int) groupID {
		listExpr := (*listExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		default:
			list := e.memo.lookupList(listExpr.items)
			return list[n-0]
		}
	},

	// orderedListOp
	func(e *expr, n int) groupID {
		orderedListExpr := (*orderedListExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		default:
			list := e.memo.lookupList(orderedListExpr.items)
			return list[n-0]
		}
	},

	// existsOp
	func(e *expr, n int) groupID {
		existsExpr := (*existsExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return existsExpr.input
		default:
			panic("child index out of range")
		}
	},

	// andOp
	func(e *expr, n int) groupID {
		andExpr := (*andExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return andExpr.left
		case 1:
			return andExpr.right
		default:
			panic("child index out of range")
		}
	},

	// orOp
	func(e *expr, n int) groupID {
		orExpr := (*orExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return orExpr.left
		case 1:
			return orExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notOp
	func(e *expr, n int) groupID {
		notExpr := (*notExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notExpr.input
		default:
			panic("child index out of range")
		}
	},

	// eqOp
	func(e *expr, n int) groupID {
		eqExpr := (*eqExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return eqExpr.left
		case 1:
			return eqExpr.right
		default:
			panic("child index out of range")
		}
	},

	// ltOp
	func(e *expr, n int) groupID {
		ltExpr := (*ltExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return ltExpr.left
		case 1:
			return ltExpr.right
		default:
			panic("child index out of range")
		}
	},

	// gtOp
	func(e *expr, n int) groupID {
		gtExpr := (*gtExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return gtExpr.left
		case 1:
			return gtExpr.right
		default:
			panic("child index out of range")
		}
	},

	// leOp
	func(e *expr, n int) groupID {
		leExpr := (*leExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return leExpr.left
		case 1:
			return leExpr.right
		default:
			panic("child index out of range")
		}
	},

	// geOp
	func(e *expr, n int) groupID {
		geExpr := (*geExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return geExpr.left
		case 1:
			return geExpr.right
		default:
			panic("child index out of range")
		}
	},

	// neOp
	func(e *expr, n int) groupID {
		neExpr := (*neExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return neExpr.left
		case 1:
			return neExpr.right
		default:
			panic("child index out of range")
		}
	},

	// inOp
	func(e *expr, n int) groupID {
		inExpr := (*inExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return inExpr.left
		case 1:
			return inExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notInOp
	func(e *expr, n int) groupID {
		notInExpr := (*notInExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notInExpr.left
		case 1:
			return notInExpr.right
		default:
			panic("child index out of range")
		}
	},

	// likeOp
	func(e *expr, n int) groupID {
		likeExpr := (*likeExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return likeExpr.left
		case 1:
			return likeExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notLikeOp
	func(e *expr, n int) groupID {
		notLikeExpr := (*notLikeExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notLikeExpr.left
		case 1:
			return notLikeExpr.right
		default:
			panic("child index out of range")
		}
	},

	// iLikeOp
	func(e *expr, n int) groupID {
		iLikeExpr := (*iLikeExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return iLikeExpr.left
		case 1:
			return iLikeExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notILikeOp
	func(e *expr, n int) groupID {
		notILikeExpr := (*notILikeExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notILikeExpr.left
		case 1:
			return notILikeExpr.right
		default:
			panic("child index out of range")
		}
	},

	// similarToOp
	func(e *expr, n int) groupID {
		similarToExpr := (*similarToExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return similarToExpr.left
		case 1:
			return similarToExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notSimilarToOp
	func(e *expr, n int) groupID {
		notSimilarToExpr := (*notSimilarToExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notSimilarToExpr.left
		case 1:
			return notSimilarToExpr.right
		default:
			panic("child index out of range")
		}
	},

	// regMatchOp
	func(e *expr, n int) groupID {
		regMatchExpr := (*regMatchExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return regMatchExpr.left
		case 1:
			return regMatchExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notRegMatchOp
	func(e *expr, n int) groupID {
		notRegMatchExpr := (*notRegMatchExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notRegMatchExpr.left
		case 1:
			return notRegMatchExpr.right
		default:
			panic("child index out of range")
		}
	},

	// regIMatchOp
	func(e *expr, n int) groupID {
		regIMatchExpr := (*regIMatchExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return regIMatchExpr.left
		case 1:
			return regIMatchExpr.right
		default:
			panic("child index out of range")
		}
	},

	// notRegIMatchOp
	func(e *expr, n int) groupID {
		notRegIMatchExpr := (*notRegIMatchExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return notRegIMatchExpr.left
		case 1:
			return notRegIMatchExpr.right
		default:
			panic("child index out of range")
		}
	},

	// isDistinctFromOp
	func(e *expr, n int) groupID {
		isDistinctFromExpr := (*isDistinctFromExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return isDistinctFromExpr.left
		case 1:
			return isDistinctFromExpr.right
		default:
			panic("child index out of range")
		}
	},

	// isNotDistinctFromOp
	func(e *expr, n int) groupID {
		isNotDistinctFromExpr := (*isNotDistinctFromExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return isNotDistinctFromExpr.left
		case 1:
			return isNotDistinctFromExpr.right
		default:
			panic("child index out of range")
		}
	},

	// isOp
	func(e *expr, n int) groupID {
		isExpr := (*isExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return isExpr.left
		case 1:
			return isExpr.right
		default:
			panic("child index out of range")
		}
	},

	// isNotOp
	func(e *expr, n int) groupID {
		isNotExpr := (*isNotExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return isNotExpr.left
		case 1:
			return isNotExpr.right
		default:
			panic("child index out of range")
		}
	},

	// anyOp
	func(e *expr, n int) groupID {
		anyExpr := (*anyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return anyExpr.left
		case 1:
			return anyExpr.right
		default:
			panic("child index out of range")
		}
	},

	// someOp
	func(e *expr, n int) groupID {
		someExpr := (*someExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return someExpr.left
		case 1:
			return someExpr.right
		default:
			panic("child index out of range")
		}
	},

	// allOp
	func(e *expr, n int) groupID {
		allExpr := (*allExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return allExpr.left
		case 1:
			return allExpr.right
		default:
			panic("child index out of range")
		}
	},

	// bitandOp
	func(e *expr, n int) groupID {
		bitandExpr := (*bitandExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return bitandExpr.left
		case 1:
			return bitandExpr.right
		default:
			panic("child index out of range")
		}
	},

	// bitorOp
	func(e *expr, n int) groupID {
		bitorExpr := (*bitorExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return bitorExpr.left
		case 1:
			return bitorExpr.right
		default:
			panic("child index out of range")
		}
	},

	// bitxorOp
	func(e *expr, n int) groupID {
		bitxorExpr := (*bitxorExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return bitxorExpr.left
		case 1:
			return bitxorExpr.right
		default:
			panic("child index out of range")
		}
	},

	// plusOp
	func(e *expr, n int) groupID {
		plusExpr := (*plusExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return plusExpr.left
		case 1:
			return plusExpr.right
		default:
			panic("child index out of range")
		}
	},

	// minusOp
	func(e *expr, n int) groupID {
		minusExpr := (*minusExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return minusExpr.left
		case 1:
			return minusExpr.right
		default:
			panic("child index out of range")
		}
	},

	// multOp
	func(e *expr, n int) groupID {
		multExpr := (*multExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return multExpr.left
		case 1:
			return multExpr.right
		default:
			panic("child index out of range")
		}
	},

	// divOp
	func(e *expr, n int) groupID {
		divExpr := (*divExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return divExpr.left
		case 1:
			return divExpr.right
		default:
			panic("child index out of range")
		}
	},

	// floorDivOp
	func(e *expr, n int) groupID {
		floorDivExpr := (*floorDivExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return floorDivExpr.left
		case 1:
			return floorDivExpr.right
		default:
			panic("child index out of range")
		}
	},

	// modOp
	func(e *expr, n int) groupID {
		modExpr := (*modExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return modExpr.left
		case 1:
			return modExpr.right
		default:
			panic("child index out of range")
		}
	},

	// powOp
	func(e *expr, n int) groupID {
		powExpr := (*powExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return powExpr.left
		case 1:
			return powExpr.right
		default:
			panic("child index out of range")
		}
	},

	// concatOp
	func(e *expr, n int) groupID {
		concatExpr := (*concatExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return concatExpr.left
		case 1:
			return concatExpr.right
		default:
			panic("child index out of range")
		}
	},

	// lShiftOp
	func(e *expr, n int) groupID {
		lShiftExpr := (*lShiftExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return lShiftExpr.left
		case 1:
			return lShiftExpr.right
		default:
			panic("child index out of range")
		}
	},

	// rShiftOp
	func(e *expr, n int) groupID {
		rShiftExpr := (*rShiftExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return rShiftExpr.left
		case 1:
			return rShiftExpr.right
		default:
			panic("child index out of range")
		}
	},

	// unaryPlusOp
	func(e *expr, n int) groupID {
		unaryPlusExpr := (*unaryPlusExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return unaryPlusExpr.input
		default:
			panic("child index out of range")
		}
	},

	// unaryMinusOp
	func(e *expr, n int) groupID {
		unaryMinusExpr := (*unaryMinusExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return unaryMinusExpr.input
		default:
			panic("child index out of range")
		}
	},

	// unaryComplementOp
	func(e *expr, n int) groupID {
		unaryComplementExpr := (*unaryComplementExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return unaryComplementExpr.input
		default:
			panic("child index out of range")
		}
	},

	// functionOp
	func(e *expr, n int) groupID {
		functionExpr := (*functionExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		default:
			list := e.memo.lookupList(functionExpr.args)
			return list[n-0]
		}
	},

	// scanOp
	func(e *expr, n int) groupID {
		panic("child index out of range")
	},

	// selectOp
	func(e *expr, n int) groupID {
		selectExpr := (*selectExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return selectExpr.input
		case 1:
			return selectExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// innerJoinOp
	func(e *expr, n int) groupID {
		innerJoinExpr := (*innerJoinExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return innerJoinExpr.left
		case 1:
			return innerJoinExpr.right
		case 2:
			return innerJoinExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// leftJoinOp
	func(e *expr, n int) groupID {
		leftJoinExpr := (*leftJoinExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return leftJoinExpr.left
		case 1:
			return leftJoinExpr.right
		case 2:
			return leftJoinExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// rightJoinOp
	func(e *expr, n int) groupID {
		rightJoinExpr := (*rightJoinExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return rightJoinExpr.left
		case 1:
			return rightJoinExpr.right
		case 2:
			return rightJoinExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// fullJoinOp
	func(e *expr, n int) groupID {
		fullJoinExpr := (*fullJoinExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return fullJoinExpr.left
		case 1:
			return fullJoinExpr.right
		case 2:
			return fullJoinExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// semiJoinOp
	func(e *expr, n int) groupID {
		semiJoinExpr := (*semiJoinExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return semiJoinExpr.left
		case 1:
			return semiJoinExpr.right
		case 2:
			return semiJoinExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// antiJoinOp
	func(e *expr, n int) groupID {
		antiJoinExpr := (*antiJoinExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return antiJoinExpr.left
		case 1:
			return antiJoinExpr.right
		case 2:
			return antiJoinExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// innerJoinApplyOp
	func(e *expr, n int) groupID {
		innerJoinApplyExpr := (*innerJoinApplyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return innerJoinApplyExpr.left
		case 1:
			return innerJoinApplyExpr.right
		case 2:
			return innerJoinApplyExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// leftJoinApplyOp
	func(e *expr, n int) groupID {
		leftJoinApplyExpr := (*leftJoinApplyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return leftJoinApplyExpr.left
		case 1:
			return leftJoinApplyExpr.right
		case 2:
			return leftJoinApplyExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// rightJoinApplyOp
	func(e *expr, n int) groupID {
		rightJoinApplyExpr := (*rightJoinApplyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return rightJoinApplyExpr.left
		case 1:
			return rightJoinApplyExpr.right
		case 2:
			return rightJoinApplyExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// fullJoinApplyOp
	func(e *expr, n int) groupID {
		fullJoinApplyExpr := (*fullJoinApplyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return fullJoinApplyExpr.left
		case 1:
			return fullJoinApplyExpr.right
		case 2:
			return fullJoinApplyExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// semiJoinApplyOp
	func(e *expr, n int) groupID {
		semiJoinApplyExpr := (*semiJoinApplyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return semiJoinApplyExpr.left
		case 1:
			return semiJoinApplyExpr.right
		case 2:
			return semiJoinApplyExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// antiJoinApplyOp
	func(e *expr, n int) groupID {
		antiJoinApplyExpr := (*antiJoinApplyExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))

		switch n {
		case 0:
			return antiJoinApplyExpr.left
		case 1:
			return antiJoinApplyExpr.right
		case 2:
			return antiJoinApplyExpr.filter
		default:
			panic("child index out of range")
		}
	},

	// sortOp
	func(e *expr, n int) groupID {
		if n == 0 {
			return e.group
		}

		panic("child index out of range")
	},

	// projectSubsetOp
	func(e *expr, n int) groupID {
		if n == 0 {
			return e.group
		}

		panic("child index out of range")
	},
}

type providedPropsLookupFunc func(e *expr) physicalPropsID

var providedPropsLookup = []providedPropsLookupFunc{
	nil, // unknownOp

	// variableOp
	defaultProvidedProps,

	// constOp
	defaultProvidedProps,

	// listOp
	defaultProvidedProps,

	// orderedListOp
	defaultProvidedProps,

	// existsOp
	defaultProvidedProps,

	// andOp
	defaultProvidedProps,

	// orOp
	defaultProvidedProps,

	// notOp
	defaultProvidedProps,

	// eqOp
	defaultProvidedProps,

	// ltOp
	defaultProvidedProps,

	// gtOp
	defaultProvidedProps,

	// leOp
	defaultProvidedProps,

	// geOp
	defaultProvidedProps,

	// neOp
	defaultProvidedProps,

	// inOp
	defaultProvidedProps,

	// notInOp
	defaultProvidedProps,

	// likeOp
	defaultProvidedProps,

	// notLikeOp
	defaultProvidedProps,

	// iLikeOp
	defaultProvidedProps,

	// notILikeOp
	defaultProvidedProps,

	// similarToOp
	defaultProvidedProps,

	// notSimilarToOp
	defaultProvidedProps,

	// regMatchOp
	defaultProvidedProps,

	// notRegMatchOp
	defaultProvidedProps,

	// regIMatchOp
	defaultProvidedProps,

	// notRegIMatchOp
	defaultProvidedProps,

	// isDistinctFromOp
	defaultProvidedProps,

	// isNotDistinctFromOp
	defaultProvidedProps,

	// isOp
	defaultProvidedProps,

	// isNotOp
	defaultProvidedProps,

	// anyOp
	defaultProvidedProps,

	// someOp
	defaultProvidedProps,

	// allOp
	defaultProvidedProps,

	// bitandOp
	defaultProvidedProps,

	// bitorOp
	defaultProvidedProps,

	// bitxorOp
	defaultProvidedProps,

	// plusOp
	defaultProvidedProps,

	// minusOp
	defaultProvidedProps,

	// multOp
	defaultProvidedProps,

	// divOp
	defaultProvidedProps,

	// floorDivOp
	defaultProvidedProps,

	// modOp
	defaultProvidedProps,

	// powOp
	defaultProvidedProps,

	// concatOp
	defaultProvidedProps,

	// lShiftOp
	defaultProvidedProps,

	// rShiftOp
	defaultProvidedProps,

	// unaryPlusOp
	defaultProvidedProps,

	// unaryMinusOp
	defaultProvidedProps,

	// unaryComplementOp
	defaultProvidedProps,

	// functionOp
	defaultProvidedProps,

	// scanOp
	defaultProvidedProps,

	// selectOp
	func(e *expr) physicalPropsID {
		selectExpr := (*selectExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))
		return selectExpr.computeProvidedProps(e.memo, e.required)
	},

	// innerJoinOp
	defaultProvidedProps,

	// leftJoinOp
	defaultProvidedProps,

	// rightJoinOp
	defaultProvidedProps,

	// fullJoinOp
	defaultProvidedProps,

	// semiJoinOp
	defaultProvidedProps,

	// antiJoinOp
	defaultProvidedProps,

	// innerJoinApplyOp
	defaultProvidedProps,

	// leftJoinApplyOp
	defaultProvidedProps,

	// rightJoinApplyOp
	defaultProvidedProps,

	// fullJoinApplyOp
	defaultProvidedProps,

	// semiJoinApplyOp
	defaultProvidedProps,

	// antiJoinApplyOp
	defaultProvidedProps,

	// sortOp
	defaultProvidedProps,

	// projectSubsetOp
	defaultProvidedProps,
}

type requiredPropsLookupFunc func(e *expr, nth int) physicalPropsID

var requiredPropsLookup = []requiredPropsLookupFunc{
	nil, // unknownOp

	// variableOp
	defaultRequiredProps,

	// constOp
	defaultRequiredProps,

	// listOp
	defaultRequiredProps,

	// orderedListOp
	defaultRequiredProps,

	// existsOp
	defaultRequiredProps,

	// andOp
	defaultRequiredProps,

	// orOp
	defaultRequiredProps,

	// notOp
	defaultRequiredProps,

	// eqOp
	defaultRequiredProps,

	// ltOp
	defaultRequiredProps,

	// gtOp
	defaultRequiredProps,

	// leOp
	defaultRequiredProps,

	// geOp
	defaultRequiredProps,

	// neOp
	defaultRequiredProps,

	// inOp
	defaultRequiredProps,

	// notInOp
	defaultRequiredProps,

	// likeOp
	defaultRequiredProps,

	// notLikeOp
	defaultRequiredProps,

	// iLikeOp
	defaultRequiredProps,

	// notILikeOp
	defaultRequiredProps,

	// similarToOp
	defaultRequiredProps,

	// notSimilarToOp
	defaultRequiredProps,

	// regMatchOp
	defaultRequiredProps,

	// notRegMatchOp
	defaultRequiredProps,

	// regIMatchOp
	defaultRequiredProps,

	// notRegIMatchOp
	defaultRequiredProps,

	// isDistinctFromOp
	defaultRequiredProps,

	// isNotDistinctFromOp
	defaultRequiredProps,

	// isOp
	defaultRequiredProps,

	// isNotOp
	defaultRequiredProps,

	// anyOp
	defaultRequiredProps,

	// someOp
	defaultRequiredProps,

	// allOp
	defaultRequiredProps,

	// bitandOp
	defaultRequiredProps,

	// bitorOp
	defaultRequiredProps,

	// bitxorOp
	defaultRequiredProps,

	// plusOp
	defaultRequiredProps,

	// minusOp
	defaultRequiredProps,

	// multOp
	defaultRequiredProps,

	// divOp
	defaultRequiredProps,

	// floorDivOp
	defaultRequiredProps,

	// modOp
	defaultRequiredProps,

	// powOp
	defaultRequiredProps,

	// concatOp
	defaultRequiredProps,

	// lShiftOp
	defaultRequiredProps,

	// rShiftOp
	defaultRequiredProps,

	// unaryPlusOp
	defaultRequiredProps,

	// unaryMinusOp
	defaultRequiredProps,

	// unaryComplementOp
	defaultRequiredProps,

	// functionOp
	defaultRequiredProps,

	// scanOp
	defaultRequiredProps,

	// selectOp
	func(e *expr, nth int) physicalPropsID {
		selectExpr := (*selectExpr)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))
		return selectExpr.computeRequiredProps(e.memo, e.required, nth)
	},

	// innerJoinOp
	defaultRequiredProps,

	// leftJoinOp
	defaultRequiredProps,

	// rightJoinOp
	defaultRequiredProps,

	// fullJoinOp
	defaultRequiredProps,

	// semiJoinOp
	defaultRequiredProps,

	// antiJoinOp
	defaultRequiredProps,

	// innerJoinApplyOp
	defaultRequiredProps,

	// leftJoinApplyOp
	defaultRequiredProps,

	// rightJoinApplyOp
	defaultRequiredProps,

	// fullJoinApplyOp
	defaultRequiredProps,

	// semiJoinApplyOp
	defaultRequiredProps,

	// antiJoinApplyOp
	defaultRequiredProps,

	// sortOp
	defaultRequiredProps,

	// projectSubsetOp
	defaultRequiredProps,
}

var isScalarLookup = []bool{
	false, // unknownOp

	true,  // variableOp
	true,  // constOp
	true,  // listOp
	true,  // orderedListOp
	true,  // existsOp
	true,  // andOp
	true,  // orOp
	true,  // notOp
	true,  // eqOp
	true,  // ltOp
	true,  // gtOp
	true,  // leOp
	true,  // geOp
	true,  // neOp
	true,  // inOp
	true,  // notInOp
	true,  // likeOp
	true,  // notLikeOp
	true,  // iLikeOp
	true,  // notILikeOp
	true,  // similarToOp
	true,  // notSimilarToOp
	true,  // regMatchOp
	true,  // notRegMatchOp
	true,  // regIMatchOp
	true,  // notRegIMatchOp
	true,  // isDistinctFromOp
	true,  // isNotDistinctFromOp
	true,  // isOp
	true,  // isNotOp
	true,  // anyOp
	true,  // someOp
	true,  // allOp
	true,  // bitandOp
	true,  // bitorOp
	true,  // bitxorOp
	true,  // plusOp
	true,  // minusOp
	true,  // multOp
	true,  // divOp
	true,  // floorDivOp
	true,  // modOp
	true,  // powOp
	true,  // concatOp
	true,  // lShiftOp
	true,  // rShiftOp
	true,  // unaryPlusOp
	true,  // unaryMinusOp
	true,  // unaryComplementOp
	true,  // functionOp
	false, // scanOp
	false, // selectOp
	false, // innerJoinOp
	false, // leftJoinOp
	false, // rightJoinOp
	false, // fullJoinOp
	false, // semiJoinOp
	false, // antiJoinOp
	false, // innerJoinApplyOp
	false, // leftJoinApplyOp
	false, // rightJoinApplyOp
	false, // fullJoinApplyOp
	false, // semiJoinApplyOp
	false, // antiJoinApplyOp
	false, // sortOp
	false, // projectSubsetOp
}

var isRelationalLookup = []bool{
	false, // unknownOp

	false, // variableOp
	false, // constOp
	false, // listOp
	false, // orderedListOp
	false, // existsOp
	false, // andOp
	false, // orOp
	false, // notOp
	false, // eqOp
	false, // ltOp
	false, // gtOp
	false, // leOp
	false, // geOp
	false, // neOp
	false, // inOp
	false, // notInOp
	false, // likeOp
	false, // notLikeOp
	false, // iLikeOp
	false, // notILikeOp
	false, // similarToOp
	false, // notSimilarToOp
	false, // regMatchOp
	false, // notRegMatchOp
	false, // regIMatchOp
	false, // notRegIMatchOp
	false, // isDistinctFromOp
	false, // isNotDistinctFromOp
	false, // isOp
	false, // isNotOp
	false, // anyOp
	false, // someOp
	false, // allOp
	false, // bitandOp
	false, // bitorOp
	false, // bitxorOp
	false, // plusOp
	false, // minusOp
	false, // multOp
	false, // divOp
	false, // floorDivOp
	false, // modOp
	false, // powOp
	false, // concatOp
	false, // lShiftOp
	false, // rShiftOp
	false, // unaryPlusOp
	false, // unaryMinusOp
	false, // unaryComplementOp
	false, // functionOp
	true,  // scanOp
	true,  // selectOp
	true,  // innerJoinOp
	true,  // leftJoinOp
	true,  // rightJoinOp
	true,  // fullJoinOp
	true,  // semiJoinOp
	true,  // antiJoinOp
	true,  // innerJoinApplyOp
	true,  // leftJoinApplyOp
	true,  // rightJoinApplyOp
	true,  // fullJoinApplyOp
	true,  // semiJoinApplyOp
	true,  // antiJoinApplyOp
	true,  // sortOp
	true,  // projectSubsetOp
}

var isProvidedPropsLookup = []bool{
	false, // unknownOp

	false, // variableOp
	false, // constOp
	false, // listOp
	false, // orderedListOp
	false, // existsOp
	false, // andOp
	false, // orOp
	false, // notOp
	false, // eqOp
	false, // ltOp
	false, // gtOp
	false, // leOp
	false, // geOp
	false, // neOp
	false, // inOp
	false, // notInOp
	false, // likeOp
	false, // notLikeOp
	false, // iLikeOp
	false, // notILikeOp
	false, // similarToOp
	false, // notSimilarToOp
	false, // regMatchOp
	false, // notRegMatchOp
	false, // regIMatchOp
	false, // notRegIMatchOp
	false, // isDistinctFromOp
	false, // isNotDistinctFromOp
	false, // isOp
	false, // isNotOp
	false, // anyOp
	false, // someOp
	false, // allOp
	false, // bitandOp
	false, // bitorOp
	false, // bitxorOp
	false, // plusOp
	false, // minusOp
	false, // multOp
	false, // divOp
	false, // floorDivOp
	false, // modOp
	false, // powOp
	false, // concatOp
	false, // lShiftOp
	false, // rShiftOp
	false, // unaryPlusOp
	false, // unaryMinusOp
	false, // unaryComplementOp
	false, // functionOp
	false, // scanOp
	true,  // selectOp
	false, // innerJoinOp
	false, // leftJoinOp
	false, // rightJoinOp
	false, // fullJoinOp
	false, // semiJoinOp
	false, // antiJoinOp
	false, // innerJoinApplyOp
	false, // leftJoinApplyOp
	false, // rightJoinApplyOp
	false, // fullJoinApplyOp
	false, // semiJoinApplyOp
	false, // antiJoinApplyOp
	false, // sortOp
	false, // projectSubsetOp
}

var isRequiredPropsLookup = []bool{
	false, // unknownOp

	false, // variableOp
	false, // constOp
	false, // listOp
	false, // orderedListOp
	false, // existsOp
	false, // andOp
	false, // orOp
	false, // notOp
	false, // eqOp
	false, // ltOp
	false, // gtOp
	false, // leOp
	false, // geOp
	false, // neOp
	false, // inOp
	false, // notInOp
	false, // likeOp
	false, // notLikeOp
	false, // iLikeOp
	false, // notILikeOp
	false, // similarToOp
	false, // notSimilarToOp
	false, // regMatchOp
	false, // notRegMatchOp
	false, // regIMatchOp
	false, // notRegIMatchOp
	false, // isDistinctFromOp
	false, // isNotDistinctFromOp
	false, // isOp
	false, // isNotOp
	false, // anyOp
	false, // someOp
	false, // allOp
	false, // bitandOp
	false, // bitorOp
	false, // bitxorOp
	false, // plusOp
	false, // minusOp
	false, // multOp
	false, // divOp
	false, // floorDivOp
	false, // modOp
	false, // powOp
	false, // concatOp
	false, // lShiftOp
	false, // rShiftOp
	false, // unaryPlusOp
	false, // unaryMinusOp
	false, // unaryComplementOp
	false, // functionOp
	false, // scanOp
	true,  // selectOp
	false, // innerJoinOp
	false, // leftJoinOp
	false, // rightJoinOp
	false, // fullJoinOp
	false, // semiJoinOp
	false, // antiJoinOp
	false, // innerJoinApplyOp
	false, // leftJoinApplyOp
	false, // rightJoinApplyOp
	false, // fullJoinApplyOp
	false, // semiJoinApplyOp
	false, // antiJoinApplyOp
	false, // sortOp
	false, // projectSubsetOp
}

var isEnforcerLookup = []bool{
	false, // unknownOp

	false, // variableOp
	false, // constOp
	false, // listOp
	false, // orderedListOp
	false, // existsOp
	false, // andOp
	false, // orOp
	false, // notOp
	false, // eqOp
	false, // ltOp
	false, // gtOp
	false, // leOp
	false, // geOp
	false, // neOp
	false, // inOp
	false, // notInOp
	false, // likeOp
	false, // notLikeOp
	false, // iLikeOp
	false, // notILikeOp
	false, // similarToOp
	false, // notSimilarToOp
	false, // regMatchOp
	false, // notRegMatchOp
	false, // regIMatchOp
	false, // notRegIMatchOp
	false, // isDistinctFromOp
	false, // isNotDistinctFromOp
	false, // isOp
	false, // isNotOp
	false, // anyOp
	false, // someOp
	false, // allOp
	false, // bitandOp
	false, // bitorOp
	false, // bitxorOp
	false, // plusOp
	false, // minusOp
	false, // multOp
	false, // divOp
	false, // floorDivOp
	false, // modOp
	false, // powOp
	false, // concatOp
	false, // lShiftOp
	false, // rShiftOp
	false, // unaryPlusOp
	false, // unaryMinusOp
	false, // unaryComplementOp
	false, // functionOp
	false, // scanOp
	false, // selectOp
	false, // innerJoinOp
	false, // leftJoinOp
	false, // rightJoinOp
	false, // fullJoinOp
	false, // semiJoinOp
	false, // antiJoinOp
	false, // innerJoinApplyOp
	false, // leftJoinApplyOp
	false, // rightJoinApplyOp
	false, // fullJoinApplyOp
	false, // semiJoinApplyOp
	false, // antiJoinApplyOp
	true,  // sortOp
	true,  // projectSubsetOp
}

func (e *expr) isScalar() bool {
	return isScalarLookup[e.op]
}

func (e *expr) isRelational() bool {
	return isRelationalLookup[e.op]
}

func (e *expr) isProvidedProps() bool {
	return isProvidedPropsLookup[e.op]
}

func (e *expr) isRequiredProps() bool {
	return isRequiredPropsLookup[e.op]
}

func (e *expr) isEnforcer() bool {
	return isEnforcerLookup[e.op]
}

type variableExpr struct {
	memoExpr
	col privateID
}

func (e *variableExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(variableExpr{})
	const offset = unsafe.Offsetof(variableExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asVariable() *variableExpr {
	if m.op != variableOp {
		return nil
	}

	return (*variableExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeVariable(e *variableExpr) groupID {
	const size = uint32(unsafe.Sizeof(variableExpr{}))
	const align = uint32(unsafe.Alignof(variableExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*variableExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type constExpr struct {
	memoExpr
	value privateID
}

func (e *constExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(constExpr{})
	const offset = unsafe.Offsetof(constExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asConst() *constExpr {
	if m.op != constOp {
		return nil
	}

	return (*constExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeConst(e *constExpr) groupID {
	const size = uint32(unsafe.Sizeof(constExpr{}))
	const align = uint32(unsafe.Alignof(constExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*constExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type listExpr struct {
	memoExpr
	items listID
}

func (e *listExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(listExpr{})
	const offset = unsafe.Offsetof(listExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asList() *listExpr {
	if m.op != listOp {
		return nil
	}

	return (*listExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeList(e *listExpr) groupID {
	const size = uint32(unsafe.Sizeof(listExpr{}))
	const align = uint32(unsafe.Alignof(listExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*listExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type orderedListExpr struct {
	memoExpr
	items listID
}

func (e *orderedListExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(orderedListExpr{})
	const offset = unsafe.Offsetof(orderedListExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asOrderedList() *orderedListExpr {
	if m.op != orderedListOp {
		return nil
	}

	return (*orderedListExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeOrderedList(e *orderedListExpr) groupID {
	const size = uint32(unsafe.Sizeof(orderedListExpr{}))
	const align = uint32(unsafe.Alignof(orderedListExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*orderedListExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type existsExpr struct {
	memoExpr
	input groupID
}

func (e *existsExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(existsExpr{})
	const offset = unsafe.Offsetof(existsExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asExists() *existsExpr {
	if m.op != existsOp {
		return nil
	}

	return (*existsExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeExists(e *existsExpr) groupID {
	const size = uint32(unsafe.Sizeof(existsExpr{}))
	const align = uint32(unsafe.Alignof(existsExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*existsExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type andExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *andExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(andExpr{})
	const offset = unsafe.Offsetof(andExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asAnd() *andExpr {
	if m.op != andOp {
		return nil
	}

	return (*andExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAnd(e *andExpr) groupID {
	const size = uint32(unsafe.Sizeof(andExpr{}))
	const align = uint32(unsafe.Alignof(andExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*andExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type orExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *orExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(orExpr{})
	const offset = unsafe.Offsetof(orExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asOr() *orExpr {
	if m.op != orOp {
		return nil
	}

	return (*orExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeOr(e *orExpr) groupID {
	const size = uint32(unsafe.Sizeof(orExpr{}))
	const align = uint32(unsafe.Alignof(orExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*orExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notExpr struct {
	memoExpr
	input groupID
}

func (e *notExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notExpr{})
	const offset = unsafe.Offsetof(notExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNot() *notExpr {
	if m.op != notOp {
		return nil
	}

	return (*notExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNot(e *notExpr) groupID {
	const size = uint32(unsafe.Sizeof(notExpr{}))
	const align = uint32(unsafe.Alignof(notExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type eqExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *eqExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(eqExpr{})
	const offset = unsafe.Offsetof(eqExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asEq() *eqExpr {
	if m.op != eqOp {
		return nil
	}

	return (*eqExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeEq(e *eqExpr) groupID {
	const size = uint32(unsafe.Sizeof(eqExpr{}))
	const align = uint32(unsafe.Alignof(eqExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*eqExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type ltExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *ltExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(ltExpr{})
	const offset = unsafe.Offsetof(ltExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asLt() *ltExpr {
	if m.op != ltOp {
		return nil
	}

	return (*ltExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLt(e *ltExpr) groupID {
	const size = uint32(unsafe.Sizeof(ltExpr{}))
	const align = uint32(unsafe.Alignof(ltExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*ltExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type gtExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *gtExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(gtExpr{})
	const offset = unsafe.Offsetof(gtExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asGt() *gtExpr {
	if m.op != gtOp {
		return nil
	}

	return (*gtExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeGt(e *gtExpr) groupID {
	const size = uint32(unsafe.Sizeof(gtExpr{}))
	const align = uint32(unsafe.Alignof(gtExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*gtExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type leExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *leExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(leExpr{})
	const offset = unsafe.Offsetof(leExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asLe() *leExpr {
	if m.op != leOp {
		return nil
	}

	return (*leExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLe(e *leExpr) groupID {
	const size = uint32(unsafe.Sizeof(leExpr{}))
	const align = uint32(unsafe.Alignof(leExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*leExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type geExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *geExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(geExpr{})
	const offset = unsafe.Offsetof(geExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asGe() *geExpr {
	if m.op != geOp {
		return nil
	}

	return (*geExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeGe(e *geExpr) groupID {
	const size = uint32(unsafe.Sizeof(geExpr{}))
	const align = uint32(unsafe.Alignof(geExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*geExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type neExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *neExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(neExpr{})
	const offset = unsafe.Offsetof(neExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNe() *neExpr {
	if m.op != neOp {
		return nil
	}

	return (*neExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNe(e *neExpr) groupID {
	const size = uint32(unsafe.Sizeof(neExpr{}))
	const align = uint32(unsafe.Alignof(neExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*neExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type inExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *inExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(inExpr{})
	const offset = unsafe.Offsetof(inExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asIn() *inExpr {
	if m.op != inOp {
		return nil
	}

	return (*inExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIn(e *inExpr) groupID {
	const size = uint32(unsafe.Sizeof(inExpr{}))
	const align = uint32(unsafe.Alignof(inExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*inExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notInExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *notInExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notInExpr{})
	const offset = unsafe.Offsetof(notInExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNotIn() *notInExpr {
	if m.op != notInOp {
		return nil
	}

	return (*notInExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotIn(e *notInExpr) groupID {
	const size = uint32(unsafe.Sizeof(notInExpr{}))
	const align = uint32(unsafe.Alignof(notInExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notInExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type likeExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *likeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(likeExpr{})
	const offset = unsafe.Offsetof(likeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asLike() *likeExpr {
	if m.op != likeOp {
		return nil
	}

	return (*likeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLike(e *likeExpr) groupID {
	const size = uint32(unsafe.Sizeof(likeExpr{}))
	const align = uint32(unsafe.Alignof(likeExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*likeExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notLikeExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *notLikeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notLikeExpr{})
	const offset = unsafe.Offsetof(notLikeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNotLike() *notLikeExpr {
	if m.op != notLikeOp {
		return nil
	}

	return (*notLikeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotLike(e *notLikeExpr) groupID {
	const size = uint32(unsafe.Sizeof(notLikeExpr{}))
	const align = uint32(unsafe.Alignof(notLikeExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notLikeExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type iLikeExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *iLikeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(iLikeExpr{})
	const offset = unsafe.Offsetof(iLikeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asILike() *iLikeExpr {
	if m.op != iLikeOp {
		return nil
	}

	return (*iLikeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeILike(e *iLikeExpr) groupID {
	const size = uint32(unsafe.Sizeof(iLikeExpr{}))
	const align = uint32(unsafe.Alignof(iLikeExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*iLikeExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notILikeExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *notILikeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notILikeExpr{})
	const offset = unsafe.Offsetof(notILikeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNotILike() *notILikeExpr {
	if m.op != notILikeOp {
		return nil
	}

	return (*notILikeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotILike(e *notILikeExpr) groupID {
	const size = uint32(unsafe.Sizeof(notILikeExpr{}))
	const align = uint32(unsafe.Alignof(notILikeExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notILikeExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type similarToExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *similarToExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(similarToExpr{})
	const offset = unsafe.Offsetof(similarToExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asSimilarTo() *similarToExpr {
	if m.op != similarToOp {
		return nil
	}

	return (*similarToExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSimilarTo(e *similarToExpr) groupID {
	const size = uint32(unsafe.Sizeof(similarToExpr{}))
	const align = uint32(unsafe.Alignof(similarToExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*similarToExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notSimilarToExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *notSimilarToExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notSimilarToExpr{})
	const offset = unsafe.Offsetof(notSimilarToExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNotSimilarTo() *notSimilarToExpr {
	if m.op != notSimilarToOp {
		return nil
	}

	return (*notSimilarToExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotSimilarTo(e *notSimilarToExpr) groupID {
	const size = uint32(unsafe.Sizeof(notSimilarToExpr{}))
	const align = uint32(unsafe.Alignof(notSimilarToExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notSimilarToExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type regMatchExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *regMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(regMatchExpr{})
	const offset = unsafe.Offsetof(regMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asRegMatch() *regMatchExpr {
	if m.op != regMatchOp {
		return nil
	}

	return (*regMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRegMatch(e *regMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(regMatchExpr{}))
	const align = uint32(unsafe.Alignof(regMatchExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*regMatchExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notRegMatchExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *notRegMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notRegMatchExpr{})
	const offset = unsafe.Offsetof(notRegMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNotRegMatch() *notRegMatchExpr {
	if m.op != notRegMatchOp {
		return nil
	}

	return (*notRegMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotRegMatch(e *notRegMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(notRegMatchExpr{}))
	const align = uint32(unsafe.Alignof(notRegMatchExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notRegMatchExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type regIMatchExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *regIMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(regIMatchExpr{})
	const offset = unsafe.Offsetof(regIMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asRegIMatch() *regIMatchExpr {
	if m.op != regIMatchOp {
		return nil
	}

	return (*regIMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRegIMatch(e *regIMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(regIMatchExpr{}))
	const align = uint32(unsafe.Alignof(regIMatchExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*regIMatchExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type notRegIMatchExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *notRegIMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notRegIMatchExpr{})
	const offset = unsafe.Offsetof(notRegIMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asNotRegIMatch() *notRegIMatchExpr {
	if m.op != notRegIMatchOp {
		return nil
	}

	return (*notRegIMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotRegIMatch(e *notRegIMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(notRegIMatchExpr{}))
	const align = uint32(unsafe.Alignof(notRegIMatchExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*notRegIMatchExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type isDistinctFromExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *isDistinctFromExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isDistinctFromExpr{})
	const offset = unsafe.Offsetof(isDistinctFromExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asIsDistinctFrom() *isDistinctFromExpr {
	if m.op != isDistinctFromOp {
		return nil
	}

	return (*isDistinctFromExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIsDistinctFrom(e *isDistinctFromExpr) groupID {
	const size = uint32(unsafe.Sizeof(isDistinctFromExpr{}))
	const align = uint32(unsafe.Alignof(isDistinctFromExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*isDistinctFromExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type isNotDistinctFromExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *isNotDistinctFromExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isNotDistinctFromExpr{})
	const offset = unsafe.Offsetof(isNotDistinctFromExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asIsNotDistinctFrom() *isNotDistinctFromExpr {
	if m.op != isNotDistinctFromOp {
		return nil
	}

	return (*isNotDistinctFromExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIsNotDistinctFrom(e *isNotDistinctFromExpr) groupID {
	const size = uint32(unsafe.Sizeof(isNotDistinctFromExpr{}))
	const align = uint32(unsafe.Alignof(isNotDistinctFromExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*isNotDistinctFromExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type isExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *isExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isExpr{})
	const offset = unsafe.Offsetof(isExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asIs() *isExpr {
	if m.op != isOp {
		return nil
	}

	return (*isExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIs(e *isExpr) groupID {
	const size = uint32(unsafe.Sizeof(isExpr{}))
	const align = uint32(unsafe.Alignof(isExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*isExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type isNotExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *isNotExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isNotExpr{})
	const offset = unsafe.Offsetof(isNotExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asIsNot() *isNotExpr {
	if m.op != isNotOp {
		return nil
	}

	return (*isNotExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIsNot(e *isNotExpr) groupID {
	const size = uint32(unsafe.Sizeof(isNotExpr{}))
	const align = uint32(unsafe.Alignof(isNotExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*isNotExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type anyExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *anyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(anyExpr{})
	const offset = unsafe.Offsetof(anyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asAny() *anyExpr {
	if m.op != anyOp {
		return nil
	}

	return (*anyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAny(e *anyExpr) groupID {
	const size = uint32(unsafe.Sizeof(anyExpr{}))
	const align = uint32(unsafe.Alignof(anyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*anyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type someExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *someExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(someExpr{})
	const offset = unsafe.Offsetof(someExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asSome() *someExpr {
	if m.op != someOp {
		return nil
	}

	return (*someExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSome(e *someExpr) groupID {
	const size = uint32(unsafe.Sizeof(someExpr{}))
	const align = uint32(unsafe.Alignof(someExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*someExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type allExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *allExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(allExpr{})
	const offset = unsafe.Offsetof(allExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asAll() *allExpr {
	if m.op != allOp {
		return nil
	}

	return (*allExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAll(e *allExpr) groupID {
	const size = uint32(unsafe.Sizeof(allExpr{}))
	const align = uint32(unsafe.Alignof(allExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*allExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type bitandExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *bitandExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(bitandExpr{})
	const offset = unsafe.Offsetof(bitandExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asBitand() *bitandExpr {
	if m.op != bitandOp {
		return nil
	}

	return (*bitandExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeBitand(e *bitandExpr) groupID {
	const size = uint32(unsafe.Sizeof(bitandExpr{}))
	const align = uint32(unsafe.Alignof(bitandExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*bitandExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type bitorExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *bitorExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(bitorExpr{})
	const offset = unsafe.Offsetof(bitorExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asBitor() *bitorExpr {
	if m.op != bitorOp {
		return nil
	}

	return (*bitorExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeBitor(e *bitorExpr) groupID {
	const size = uint32(unsafe.Sizeof(bitorExpr{}))
	const align = uint32(unsafe.Alignof(bitorExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*bitorExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type bitxorExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *bitxorExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(bitxorExpr{})
	const offset = unsafe.Offsetof(bitxorExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asBitxor() *bitxorExpr {
	if m.op != bitxorOp {
		return nil
	}

	return (*bitxorExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeBitxor(e *bitxorExpr) groupID {
	const size = uint32(unsafe.Sizeof(bitxorExpr{}))
	const align = uint32(unsafe.Alignof(bitxorExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*bitxorExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type plusExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *plusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(plusExpr{})
	const offset = unsafe.Offsetof(plusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asPlus() *plusExpr {
	if m.op != plusOp {
		return nil
	}

	return (*plusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizePlus(e *plusExpr) groupID {
	const size = uint32(unsafe.Sizeof(plusExpr{}))
	const align = uint32(unsafe.Alignof(plusExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*plusExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type minusExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *minusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(minusExpr{})
	const offset = unsafe.Offsetof(minusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asMinus() *minusExpr {
	if m.op != minusOp {
		return nil
	}

	return (*minusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeMinus(e *minusExpr) groupID {
	const size = uint32(unsafe.Sizeof(minusExpr{}))
	const align = uint32(unsafe.Alignof(minusExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*minusExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type multExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *multExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(multExpr{})
	const offset = unsafe.Offsetof(multExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asMult() *multExpr {
	if m.op != multOp {
		return nil
	}

	return (*multExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeMult(e *multExpr) groupID {
	const size = uint32(unsafe.Sizeof(multExpr{}))
	const align = uint32(unsafe.Alignof(multExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*multExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type divExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *divExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(divExpr{})
	const offset = unsafe.Offsetof(divExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asDiv() *divExpr {
	if m.op != divOp {
		return nil
	}

	return (*divExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeDiv(e *divExpr) groupID {
	const size = uint32(unsafe.Sizeof(divExpr{}))
	const align = uint32(unsafe.Alignof(divExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*divExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type floorDivExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *floorDivExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(floorDivExpr{})
	const offset = unsafe.Offsetof(floorDivExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asFloorDiv() *floorDivExpr {
	if m.op != floorDivOp {
		return nil
	}

	return (*floorDivExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeFloorDiv(e *floorDivExpr) groupID {
	const size = uint32(unsafe.Sizeof(floorDivExpr{}))
	const align = uint32(unsafe.Alignof(floorDivExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*floorDivExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type modExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *modExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(modExpr{})
	const offset = unsafe.Offsetof(modExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asMod() *modExpr {
	if m.op != modOp {
		return nil
	}

	return (*modExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeMod(e *modExpr) groupID {
	const size = uint32(unsafe.Sizeof(modExpr{}))
	const align = uint32(unsafe.Alignof(modExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*modExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type powExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *powExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(powExpr{})
	const offset = unsafe.Offsetof(powExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asPow() *powExpr {
	if m.op != powOp {
		return nil
	}

	return (*powExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizePow(e *powExpr) groupID {
	const size = uint32(unsafe.Sizeof(powExpr{}))
	const align = uint32(unsafe.Alignof(powExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*powExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type concatExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *concatExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(concatExpr{})
	const offset = unsafe.Offsetof(concatExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asConcat() *concatExpr {
	if m.op != concatOp {
		return nil
	}

	return (*concatExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeConcat(e *concatExpr) groupID {
	const size = uint32(unsafe.Sizeof(concatExpr{}))
	const align = uint32(unsafe.Alignof(concatExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*concatExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type lShiftExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *lShiftExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(lShiftExpr{})
	const offset = unsafe.Offsetof(lShiftExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asLShift() *lShiftExpr {
	if m.op != lShiftOp {
		return nil
	}

	return (*lShiftExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLShift(e *lShiftExpr) groupID {
	const size = uint32(unsafe.Sizeof(lShiftExpr{}))
	const align = uint32(unsafe.Alignof(lShiftExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*lShiftExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type rShiftExpr struct {
	memoExpr
	left  groupID
	right groupID
}

func (e *rShiftExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(rShiftExpr{})
	const offset = unsafe.Offsetof(rShiftExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asRShift() *rShiftExpr {
	if m.op != rShiftOp {
		return nil
	}

	return (*rShiftExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRShift(e *rShiftExpr) groupID {
	const size = uint32(unsafe.Sizeof(rShiftExpr{}))
	const align = uint32(unsafe.Alignof(rShiftExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*rShiftExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type unaryPlusExpr struct {
	memoExpr
	input groupID
}

func (e *unaryPlusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(unaryPlusExpr{})
	const offset = unsafe.Offsetof(unaryPlusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asUnaryPlus() *unaryPlusExpr {
	if m.op != unaryPlusOp {
		return nil
	}

	return (*unaryPlusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeUnaryPlus(e *unaryPlusExpr) groupID {
	const size = uint32(unsafe.Sizeof(unaryPlusExpr{}))
	const align = uint32(unsafe.Alignof(unaryPlusExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*unaryPlusExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type unaryMinusExpr struct {
	memoExpr
	input groupID
}

func (e *unaryMinusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(unaryMinusExpr{})
	const offset = unsafe.Offsetof(unaryMinusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asUnaryMinus() *unaryMinusExpr {
	if m.op != unaryMinusOp {
		return nil
	}

	return (*unaryMinusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeUnaryMinus(e *unaryMinusExpr) groupID {
	const size = uint32(unsafe.Sizeof(unaryMinusExpr{}))
	const align = uint32(unsafe.Alignof(unaryMinusExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*unaryMinusExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type unaryComplementExpr struct {
	memoExpr
	input groupID
}

func (e *unaryComplementExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(unaryComplementExpr{})
	const offset = unsafe.Offsetof(unaryComplementExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asUnaryComplement() *unaryComplementExpr {
	if m.op != unaryComplementOp {
		return nil
	}

	return (*unaryComplementExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeUnaryComplement(e *unaryComplementExpr) groupID {
	const size = uint32(unsafe.Sizeof(unaryComplementExpr{}))
	const align = uint32(unsafe.Alignof(unaryComplementExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*unaryComplementExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type functionExpr struct {
	memoExpr
	args listID
	def  privateID
}

func (e *functionExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(functionExpr{})
	const offset = unsafe.Offsetof(functionExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asFunction() *functionExpr {
	if m.op != functionOp {
		return nil
	}

	return (*functionExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeFunction(e *functionExpr) groupID {
	const size = uint32(unsafe.Sizeof(functionExpr{}))
	const align = uint32(unsafe.Alignof(functionExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*functionExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type scanExpr struct {
	memoExpr
	table privateID
}

func (e *scanExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(scanExpr{})
	const offset = unsafe.Offsetof(scanExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asScan() *scanExpr {
	if m.op != scanOp {
		return nil
	}

	return (*scanExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeScan(e *scanExpr) groupID {
	const size = uint32(unsafe.Sizeof(scanExpr{}))
	const align = uint32(unsafe.Alignof(scanExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*scanExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type selectExpr struct {
	memoExpr
	input  groupID
	filter groupID
}

func (e *selectExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(selectExpr{})
	const offset = unsafe.Offsetof(selectExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asSelect() *selectExpr {
	if m.op != selectOp {
		return nil
	}

	return (*selectExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSelect(e *selectExpr) groupID {
	const size = uint32(unsafe.Sizeof(selectExpr{}))
	const align = uint32(unsafe.Alignof(selectExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*selectExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type innerJoinExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *innerJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(innerJoinExpr{})
	const offset = unsafe.Offsetof(innerJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asInnerJoin() *innerJoinExpr {
	if m.op != innerJoinOp {
		return nil
	}

	return (*innerJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeInnerJoin(e *innerJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(innerJoinExpr{}))
	const align = uint32(unsafe.Alignof(innerJoinExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*innerJoinExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type leftJoinExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *leftJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(leftJoinExpr{})
	const offset = unsafe.Offsetof(leftJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asLeftJoin() *leftJoinExpr {
	if m.op != leftJoinOp {
		return nil
	}

	return (*leftJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLeftJoin(e *leftJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(leftJoinExpr{}))
	const align = uint32(unsafe.Alignof(leftJoinExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*leftJoinExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type rightJoinExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *rightJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(rightJoinExpr{})
	const offset = unsafe.Offsetof(rightJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asRightJoin() *rightJoinExpr {
	if m.op != rightJoinOp {
		return nil
	}

	return (*rightJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRightJoin(e *rightJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(rightJoinExpr{}))
	const align = uint32(unsafe.Alignof(rightJoinExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*rightJoinExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type fullJoinExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *fullJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(fullJoinExpr{})
	const offset = unsafe.Offsetof(fullJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asFullJoin() *fullJoinExpr {
	if m.op != fullJoinOp {
		return nil
	}

	return (*fullJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeFullJoin(e *fullJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(fullJoinExpr{}))
	const align = uint32(unsafe.Alignof(fullJoinExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*fullJoinExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type semiJoinExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *semiJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(semiJoinExpr{})
	const offset = unsafe.Offsetof(semiJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asSemiJoin() *semiJoinExpr {
	if m.op != semiJoinOp {
		return nil
	}

	return (*semiJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSemiJoin(e *semiJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(semiJoinExpr{}))
	const align = uint32(unsafe.Alignof(semiJoinExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*semiJoinExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type antiJoinExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *antiJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(antiJoinExpr{})
	const offset = unsafe.Offsetof(antiJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asAntiJoin() *antiJoinExpr {
	if m.op != antiJoinOp {
		return nil
	}

	return (*antiJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAntiJoin(e *antiJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(antiJoinExpr{}))
	const align = uint32(unsafe.Alignof(antiJoinExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*antiJoinExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type innerJoinApplyExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *innerJoinApplyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(innerJoinApplyExpr{})
	const offset = unsafe.Offsetof(innerJoinApplyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asInnerJoinApply() *innerJoinApplyExpr {
	if m.op != innerJoinApplyOp {
		return nil
	}

	return (*innerJoinApplyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeInnerJoinApply(e *innerJoinApplyExpr) groupID {
	const size = uint32(unsafe.Sizeof(innerJoinApplyExpr{}))
	const align = uint32(unsafe.Alignof(innerJoinApplyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*innerJoinApplyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type leftJoinApplyExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *leftJoinApplyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(leftJoinApplyExpr{})
	const offset = unsafe.Offsetof(leftJoinApplyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asLeftJoinApply() *leftJoinApplyExpr {
	if m.op != leftJoinApplyOp {
		return nil
	}

	return (*leftJoinApplyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLeftJoinApply(e *leftJoinApplyExpr) groupID {
	const size = uint32(unsafe.Sizeof(leftJoinApplyExpr{}))
	const align = uint32(unsafe.Alignof(leftJoinApplyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*leftJoinApplyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type rightJoinApplyExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *rightJoinApplyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(rightJoinApplyExpr{})
	const offset = unsafe.Offsetof(rightJoinApplyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asRightJoinApply() *rightJoinApplyExpr {
	if m.op != rightJoinApplyOp {
		return nil
	}

	return (*rightJoinApplyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRightJoinApply(e *rightJoinApplyExpr) groupID {
	const size = uint32(unsafe.Sizeof(rightJoinApplyExpr{}))
	const align = uint32(unsafe.Alignof(rightJoinApplyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*rightJoinApplyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type fullJoinApplyExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *fullJoinApplyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(fullJoinApplyExpr{})
	const offset = unsafe.Offsetof(fullJoinApplyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asFullJoinApply() *fullJoinApplyExpr {
	if m.op != fullJoinApplyOp {
		return nil
	}

	return (*fullJoinApplyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeFullJoinApply(e *fullJoinApplyExpr) groupID {
	const size = uint32(unsafe.Sizeof(fullJoinApplyExpr{}))
	const align = uint32(unsafe.Alignof(fullJoinApplyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*fullJoinApplyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type semiJoinApplyExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *semiJoinApplyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(semiJoinApplyExpr{})
	const offset = unsafe.Offsetof(semiJoinApplyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asSemiJoinApply() *semiJoinApplyExpr {
	if m.op != semiJoinApplyOp {
		return nil
	}

	return (*semiJoinApplyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSemiJoinApply(e *semiJoinApplyExpr) groupID {
	const size = uint32(unsafe.Sizeof(semiJoinApplyExpr{}))
	const align = uint32(unsafe.Alignof(semiJoinApplyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*semiJoinApplyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}

type antiJoinApplyExpr struct {
	memoExpr
	left   groupID
	right  groupID
	filter groupID
}

func (e *antiJoinApplyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(antiJoinApplyExpr{})
	const offset = unsafe.Offsetof(antiJoinApplyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (m *memoExpr) asAntiJoinApply() *antiJoinApplyExpr {
	if m.op != antiJoinApplyOp {
		return nil
	}

	return (*antiJoinApplyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAntiJoinApply(e *antiJoinApplyExpr) groupID {
	const size = uint32(unsafe.Sizeof(antiJoinApplyExpr{}))
	const align = uint32(unsafe.Alignof(antiJoinApplyExpr{}))

	fingerprint := e.fingerprint()
	loc := m.exprMap[fingerprint]
	if loc.offset == 0 {
		loc.offset = exprOffset(m.arena.Alloc(size, align))

		if loc.group == 0 {
			if e.group != 0 {
				loc.group = e.group
			} else {
				loc.group = m.newGroup(e, loc.offset)
			}
		} else {
			if e.group != loc.group {
				panic("denormalized expression's group doesn't match fingerprint group")
			}
		}

		p := (*antiJoinApplyExpr)(m.arena.GetPointer(uint32(loc.offset)))
		*p = *e

		p.group = loc.group

		m.lookupGroup(loc.group).addExpr(loc.offset)
		m.exprMap[fingerprint] = loc
	}

	return loc.group
}
