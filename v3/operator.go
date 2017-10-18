package v3

import (
	"bytes"
	"fmt"
)

type operator int16

const (
	unknownOp operator = iota

	scanOp
	renameOp

	unionOp
	intersectOp
	exceptOp

	innerJoinOp
	leftJoinOp
	rightJoinOp
	fullJoinOp
	semiJoinOp
	antiJoinOp

	projectOp
	selectOp

	groupByOp
	orderByOp
	distinctOp

	variableOp
	constOp

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
	inOp
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

	bitandOp
	bitorOp
	bitxorOp
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

	numOperators
)

type operatorInfo interface {
	format(e *expr, buf *bytes.Buffer, level int)
	updateProperties(e *expr)
}

var (
	operatorTab = [numOperators]operatorInfo{}

	operatorNames = [numOperators]string{
		unknownOp: "unknown",
	}
)

func (o operator) String() string {
	if o < 0 || o > operator(len(operatorNames)-1) {
		return fmt.Sprintf("operator(%d)", o)
	}
	return operatorNames[o]
}

func registerOperator(op operator, name string, info operatorInfo) {
	operatorNames[op] = name
	operatorTab[op] = info
}
