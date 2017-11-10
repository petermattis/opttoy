package v3

import "bytes"

// The pattern operator allows defining patterns in terms of the expression
// structure. A pattern expression is used to extract an actual expression from
// the memo in order to perform a transformation on it. The pattern operator
// indicates where recursive extraction of the full subtree is required. For
// example, the join commutativity transformation wants to extract
// expressions. It does not care about the inputs, but only wants to reorder
// them. This can be specified by the pattern expression:
//
//   &expr{op: innerJoinOp}
//
// Join associativity is somewhat more complicated in that it wants a join
// where the left input is also a join:
//
//   &expr{op: innerJoinOp, inputs; []*expr{{op: innerJoinOp}, nil}}
//
// Only the matching portions of the expression are extracted. If the memoExpr
// contains additional fields such as filters, those are extracted as
// placeholder expressions with op==patternOp.

func init() {
	registerOperator(patternOp, "pattern", pattern{})
}

type pattern struct{}

func (pattern) kind() operatorKind {
	return relationalKind
}

func (pattern) format(e *expr, buf *bytes.Buffer, level int) {
}

func (pattern) initKeys(e *expr, state *queryState) {
}

func (pattern) updateProps(e *expr) {
}

func (pattern) requiredInputVars(e *expr) bitmap {
	return 0
}
