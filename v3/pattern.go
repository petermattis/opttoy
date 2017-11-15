package v3

// A pattern expression is used to extract expressions from the memo in order to
// perform a transformation on them. The pattern expression describes what the
// structure of the expression needs to be for a given transformation to apply.
// A memo expression can have many bindings that satisfy a given pattern.
//
// To help define these patterns, we define pattern tree sentinels which can be
// used as leaves of the pattern expression tree.
//
//  - Pattern Leaf
//
//  A Pattern Leaf matches any expression tree, with only the root of the tree
//  being retained in a binding. It is used when the expression is used opaquely
//  by the transformation. In other words, the transformation doesn't care
//  what's inside the subtree. It is a "leaf" in the sense that it's a leaf in
//  any binding matching a pattern.
//
//  - Pattern Tree
//
//  A Pattern Tree matches any expression tree and indicates that recursive
//  extraction of the full subtree is required. It is typically used for scalar
//  expressions, when some manipulation of that expression is required by the
//  transformation. Note that a pattern tree results in all possible subtrees
//  being enumerated, however scalar expressions typically don't have many
//  subtrees (e.g. if there are no subqueries, there is only one subtree).
//
// Examples:
//
//   Join commutativity: the transformation wants to extract the child
//   expressions (left, right, ON condition) but does not care what's inside
//   them, it just wants to reorder them. The pattern expression for this
//   transformation is:
//
//     inner join
//      |
//      |-- pattern leaf
//      |
//      |-- pattern leaf
//      |
//      |-- pattern leaf
//
//   Join associativity: the transformation wants a join where the left input is
//   also a join; in addition, it needs to adjust the ON conditions. The pattern
//   for this transformation is:
//
//     inner join
//      |
//      |-- inner join
//      |    |
//      |    |-- pattern leaf
//      |    |
//      |    |-- pattern leaf
//      |    |
//      |    |-- pattern tree
//      |
//      |
//      |-- pattern leaf
//      |
//      |-- pattern tree
//
//    Note the use of "pattern tree" for the ON conditions.

var patternLeaf = &expr{}
var patternTree = &expr{}

func isPatternSentinel(pattern *expr) bool {
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
	if isPatternSentinel(pattern) {
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
		} else if !isPatternSentinel(pattern.children[i]) {
			return false
		}
	}
	return true
}

func childPattern(pattern *expr, childIdx int) *expr {
	if isPatternTree(pattern) {
		return patternTree
	}
	return pattern.children[childIdx]
}
