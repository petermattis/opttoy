package v3

import (
	"bytes"
	"fmt"
)

type operator int16

const (
	unknownOp operator = iota

	// Relational operators
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

	groupByOp
	orderByOp

	// Scalar operators
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

	// TODO(peter): Pattern operators. The idea here is to define patterns in
	// terms of the expression structure. patternLeaf allows the extraction of an
	// expression tree for which we only care about the logical properties of the
	// node. patternTree allows for the recursive extraction of the full
	// subtree. Any other operator must match precisely.
	//
	// Consider a transformation that wants to match on groupByOp ->
	// innerJoinOp. This would be expressed as:
	//
	//   groupByOp
	//     groupings:     patternTree
	//     aggregations:  patternTree
	//     filters:       patternTree
	//     inputs:
	//       innerJoinOp
	//         filters:   patternTree
	//       inputs:      patternLeaf
	patternLeaf
	patternTree

	numOperators
)

type operatorInfo interface {
	format(e *expr, buf *bytes.Buffer, level int)
	updateProps(e *expr)
}

var (
	operatorTab = [numOperators]operatorInfo{}

	operatorNames = [numOperators]string{
		unknownOp: "unknown",
	}
)

func (op operator) String() string {
	if op < 0 || op > operator(len(operatorNames)-1) {
		return fmt.Sprintf("operator(%d)", op)
	}
	return operatorNames[op]
}

func registerOperator(op operator, name string, info operatorInfo) {
	operatorNames[op] = name
	operatorTab[op] = info
}
