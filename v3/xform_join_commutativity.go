package v3

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

// RS -> SR
func (xformJoinCommutativity) apply(e *expr, results []*expr) []*expr {
	t := newJoinExpr(e.op, e.children[1], e.children[0])
	t.children[2] = e.children[2]
	t.props = e.props
	return append(results, t)
}
