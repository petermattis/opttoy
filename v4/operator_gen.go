package v4

import (
	"unsafe"
)

const (
	unknownOp operator = iota

	variableOp
	constOp
	listOp
	orderedListOp
	existsOp
	andOp
	orOp
	notOp
	eqOp
	ltOp
	gtOp
	leOp
	geOp
	neOp
	inOpOp
	notInOp
	likeOp
	notLikeOp
	iLikeOp
	notILikeOp
	similarToOp
	notSimilarToOp
	regMatchOp
	notRegMatchOp
	regIMatchOp
	notRegIMatchOp
	isDistinctFromOp
	isNotDistinctFromOp
	isOp
	isNotOp
	anyOp
	someOp
	allOp
	bitAndOp
	bitOrOp
	bitXorOp
	plusOp
	minusOp
	multOp
	divOp
	floorDivOp
	modOp
	powOp
	concatOp
	lShiftOp
	rShiftOp
	unaryPlusOp
	unaryMinusOp
	unaryComplementOp
	functionOp
	innerJoinOp
)

type opConvertFunc func(m *memoExpr) expr

var opConvertLookup = []opConvertFunc{
	nil,

	func(m *memoExpr) expr { return (*variableExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*constExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*listExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*orderedListExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*existsExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*andExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*orExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*eqExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*ltExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*gtExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*leExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*geExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*neExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*inOpExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notInExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*likeExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notLikeExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*iLikeExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notILikeExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*similarToExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notSimilarToExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*regMatchExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notRegMatchExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*regIMatchExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*notRegIMatchExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*isDistinctFromExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*isNotDistinctFromExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*isExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*isNotExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*anyExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*someExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*allExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*bitAndExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*bitOrExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*bitXorExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*plusExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*minusExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*multExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*divExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*floorDivExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*modExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*powExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*concatExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*lShiftExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*rShiftExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*unaryPlusExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*unaryMinusExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*unaryComplementExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*functionExpr)(unsafe.Pointer(m)) },
	func(m *memoExpr) expr { return (*innerJoinExpr)(unsafe.Pointer(m)) },
}

func (m *memoExpr) asExpr() expr {
	return opConvertLookup[m.op](m)
}
