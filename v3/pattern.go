package v3

// The pattern leaf and pattern tree sentinels allow definining patterns in
// terms of the expression structure. A pattern expression is used to extract
// an actual expression from the memo in order to perform a transformation on
// it. (See memo.{bind,advance}). The pattern sentinels indicate where
// recursive extraction of the full subtree is required. For example, the join
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

var patternLeaf = &expr{}
var patternTree = &expr{}

func isPatternExpr(pattern *expr) bool {
	return isPatternLeaf(pattern) || isPatternTree(pattern)
}

func isPatternLeaf(pattern *expr) bool {
	return pattern == patternLeaf
}

func isPatternTree(pattern *expr) bool {
	return pattern == patternTree
}
