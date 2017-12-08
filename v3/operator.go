package v3

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
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
	placeholderOp
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

type operatorClass interface {
	kind() operatorKind
	format(e *expr, tp treeprinter.Node)

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

type operatorInfo struct {
	name   string
	class  operatorClass
	layout exprLayout
}

var operatorTab = [numOperators]operatorInfo{
	unknownOp: operatorInfo{name: "unknown"},
}

func (op operator) String() string {
	if op < 0 || op >= numOperators {
		return fmt.Sprintf("operator(%d)", op)
	}
	return operatorTab[op].name
}

func registerOperator(op operator, name string, class operatorClass) {
	operatorTab[op].name = name
	operatorTab[op].class = class

	if class != nil {
		// Normalize the layout so that auxiliary expressions that are not present
		// are given an invalid index which will cause a panic if they are accessed.
		l := class.layout()
		if l.numAux == 0 {
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
		}
		operatorTab[op].layout = l
	}
}
