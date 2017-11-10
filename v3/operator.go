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
	listOp

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

	// Pattern operator
	patternOp

	numOperators
)

type operatorKind int

const (
	_ operatorKind = iota
	relationalKind
	scalarKind
)

type operatorInfo interface {
	kind() operatorKind
	format(e *expr, buf *bytes.Buffer, level int)

	// Initialize keys and foreign keys in the logical properties.
	initKeys(e *expr, state *queryState)

	// Update the logical properties based on the internal state of the
	// expression.
	updateProps(e *expr)

	// Required input vars is the set of input variables that the expression
	// requires.
	requiredInputVars(e *expr) bitmap
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
