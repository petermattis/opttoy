package v3

func init() {
	registerXform(joinCommutativity{})
}

type joinCommutativity struct {
	xformExploration
}

func (joinCommutativity) id() xformID {
	return xformJoinCommutativityID
}

func (joinCommutativity) pattern() *expr {
	return &expr{
		op: innerJoinOp,
		children: []*expr{
			patternLeaf, /* left */
			patternLeaf, /* right */
			patternTree, /* filter */
		},
	}
}

// RS -> SR
func (joinCommutativity) apply(e *expr, results []*expr) []*expr {
	t := newJoinExpr(e.op, e.children[1], e.children[0])
	t.children[2] = e.children[2]
	t.props = e.props
	return append(results, t)
}
