package v3

import (
	"bytes"
	"fmt"
)

// The pattern operator allows defining patterns in terms of the expression
// structure. A pattern expression is used to extract an actual expression from
// the memo in order to perform a transformation on it. (See
// memo.{bind,advance}). The pattern operator indicates where recursive
// extraction of the full subtree is required. For example, the join
// commutativity transformation wants to extract expressions. It does not care
// about the inputs, but only wants to reorder them. This can be specified by
// the pattern expression:
//
// inner join
//   0: pattern leaf
//   1: pattern leaf
//   2: pattern leaf
//
// Join associativity is somewhat more complicated in that it wants a join
// where the left input is also a join:
//
// inner join
//   0: inner join
//     0: pattern leaf
//     1: pattern leaf
//     2: pattern tree
//   1: pattern leaf
//   2: pattern tree
//
// Note that "pattern leaf" is represented by a nil expression while "pattern
// tree" is represnted by expr.op==patternOp.

func init() {
	registerOperator(patternOp, "pattern", pattern{})
}

var patternExpr = &expr{op: patternOp}

type pattern struct{}

func (pattern) kind() operatorKind {
	return relationalKind
}

func (pattern) format(e *expr, buf *bytes.Buffer, level int) {
	fmt.Fprintf(buf, "%s%s\n", spaces[level*2], e.op)
}

func (pattern) initKeys(e *expr, state *queryState) {
}

func (pattern) updateProps(e *expr) {
}

func (pattern) requiredInputVars(e *expr) bitmap {
	return 0
}

func isPatternOp(pattern *expr) bool {
	return isPatternLeaf(pattern) || isPatternTree(pattern)
}

func isPatternLeaf(pattern *expr) bool {
	return pattern == nil
}

func isPatternTree(pattern *expr) bool {
	return pattern.op == patternOp
}
