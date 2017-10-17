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

type operatorInfo struct {
	name             string
	format           func(e *expr, buf *bytes.Buffer, level int)
	updateProperties func(e *expr)
}

var operatorTab = [numOperators]operatorInfo{
	unknownOp: {name: "unknown"},
}

func (o operator) String() string {
	if o < 0 || o > operator(len(operatorTab)-1) {
		return fmt.Sprintf("operator(%d)", o)
	}
	return operatorTab[o].name
}
