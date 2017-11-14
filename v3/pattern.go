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

// patternMatch determines if a pattern expression matches the specified
// expression. Used for matching a pattern against an expression outside of the
// memo. If an expression has been extracted from the memo using memo.bind() it
// is not necessary to match against the pattern used for extraction again.
func patternMatch(pattern, e *expr) bool {
	if isPatternExpr(pattern) {
		return true
	}
	if pattern.op != e.op {
		return false
	}
	if len(pattern.children) != len(e.children) {
		return false
	}
	for i := range pattern.children {
		if e.children[i] != nil {
			if !patternMatch(pattern.children[i], e.children[i]) {
				return false
			}
		} else if !isPatternExpr(pattern.children[i]) {
			return false
		}
	}
	return true
}
