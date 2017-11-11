package v3

type xformID int32

const (
	xformJoinCommutativityID xformID = iota

	numXforms
)

// xform defines the interface for transformations. Every transformation has a
// unique ID allowing easy determination of which transformations have been
// applied to which memo expressions. Transformations are categorized as either
// exploration or implementation. Exploration transformations create new
// logical expressions. Implementation transformations create implementation
// expressions (e.g. merge join, hash join, index scan, etc). Some
// transformations are both implementation and exploration (e.g. scalar
// normalization transforms).
//
// Transformations specify a pattern expression which is used to extract
// expressions from the memo to transform. The root of a pattern expression
// must be a concrete operator (i.e. it can't be patternOp or nil). A check()
// method can provide additional checking for whether the transformation can be
// applied to a specific expression.
//
// TODO(peter): Allow transforms can specify their compatibility with other
// transforms. Memo expressions store which transformation created them which
// allows skipping transforms. For example, the join commutativity transform is
// not applied to an expression that was created by the join commutatitivity
// transform.
type xform interface {
	// The ID of the transform.
	id() xformID

	// Is this an exploration transform?
	exploration() bool

	// Is this an implementation transform?
	implementation() bool

	// The pattern expression that is used to extract an expression from the
	// memo. Check and apply will only be called for expressions that match the
	// pattern.
	pattern() *expr

	// Check whether the transform can be applied to an expression.
	check(e *expr) bool

	// Apply the transform to an expression, producing zero or more new
	// expressions. The output expressions are appended to the results slice and
	// the results slice returned.
	apply(e *expr, results []*expr) []*expr
}

type xformExploration struct{}

func (xformExploration) exploration() bool {
	return true
}

func (xformExploration) implementation() bool {
	return false
}

type xformImplementation struct{}

func (xformImplementation) exploration() bool {
	return false
}

func (xformImplementation) implementation() bool {
	return true
}

var xforms = [numXforms]xform{}
var explorationXforms = [numOperators][]xformID{}
var implementationXforms = [numOperators][]xformID{}

func registerXform(xform xform) {
	p := xform.pattern()
	if isPatternOp(p) {
		fatalf("patterns need to be rooted in a non-pattern operator: %s", p)
	}

	xforms[xform.id()] = xform

	// Add the transform to the per-op lists of exploration and implementation
	// transforms.
	if xform.exploration() {
		explorationXforms[p.op] = append(explorationXforms[p.op], xform.id())
	}
	if xform.implementation() {
		implementationXforms[p.op] = append(implementationXforms[p.op], xform.id())
	}
}

func init() {
	registerXform(xformJoinCommutativity{})
}

type xformJoinCommutativity struct {
	xformImplementation
}

func (xformJoinCommutativity) id() xformID {
	return xformJoinCommutativityID
}

func (xformJoinCommutativity) pattern() *expr {
	return newJoinPattern(innerJoinOp, nil, nil, patternTree)
}

func (xformJoinCommutativity) check(e *expr) bool {
	return true
}

func (xformJoinCommutativity) apply(e *expr, results []*expr) []*expr {
	t := newJoinExpr(e.op, e.children[1], e.children[0])
	t.children[2] = e.children[2]
	t.props = e.props
	return append(results, t)
}
