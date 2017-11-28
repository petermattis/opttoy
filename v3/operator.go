package v3

import (
	"bytes"
	"fmt"
)

type operator uint8

const (
	unknownOp operator = iota

	// Relational operators
	scanOp
	selectOp

	unionOp
	intersectOp
	exceptOp

	innerJoinOp
	leftJoinOp
	rightJoinOp
	fullJoinOp
	semiJoinOp
	antiJoinOp

	innerJoinApplyOp
	leftJoinApplyOp
	rightJoinApplyOp
	fullJoinApplyOp
	semiJoinApplyOp
	antiJoinApplyOp

	projectOp

	groupByOp
	orderByOp

	// Scalar operators
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

	functionOp

	// Physical operators
	indexJoinOp
	indexScanOp

	sortOp

	numOperators
)

type operatorKind int

const (
	logicalKind operatorKind = 1 << iota
	physicalKind
	relationalKind
	scalarKind
)

type operatorInfo interface {
	kind() operatorKind
	format(e *expr, buf *bytes.Buffer, level int)

	// The layout of auxiliary expressions.
	layout() exprLayout

	// Initialize keys and foreign keys in the relational properties.
	initKeys(e *expr, state *queryState)

	// Update the logical properties based on the internal state of the
	// expression.
	updateProps(e *expr)

	// Compute the required physical properties for the specified child given
	// required properties on the receiver.
	requiredProps(required *physicalProps, child int) *physicalProps
}

var (
	operatorTab = [numOperators]operatorInfo{}

	operatorLayout = [numOperators]exprLayout{}

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

	if info != nil {
		// Normalize the layout so that auxiliary expressions that are not present
		// are given an invalid index which will cause a panic if they are accessed.
		l := info.layout()
		if l.aggregations == 0 {
			l.aggregations = -1
		} else {
			l.numAux++
		}
		if l.groupings == 0 {
			l.groupings = -1
		} else {
			l.numAux++
		}
		if l.projections == 0 {
			l.projections = -1
		} else {
			l.numAux++
		}
		if l.filters == 0 {
			l.filters = -1
		} else {
			l.numAux++
		}
		operatorLayout[op] = l
	}
}
