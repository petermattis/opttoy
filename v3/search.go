package v3

// Search is modelled as a series of tasks that optimize an
// expression. Conceptually, the tasks form a dependency tree very much like
// the dependency tree formed by tools like make. The current implementation of
// dependencies is implicit. [TODO(peter): need to fix this]
//
// Search begins with optimization of the group for the root expression.
//
//   1. optimizeGroup: implements the group (via implementGroup) which
//      generates implementations for the expressions in the group, then
//      selects the plan with the least estimated cost.
//
//   2. optimizeGroupExpr: optimizes all of the child groups (via
//      optimizeGroup), then selects the plan rooted at the group expression
//      with the least estimated cost.
//
//   3. implementGroup: explores the group (via exploreGroup) which generates
//      more logical expressions in the group, then generates implementations
//      for all of the logical expressions.
//
//   4. implementGroupExpr: implements all of the child groups (via
//      implementGroup), then applies any applicable implementation
//      transformations to the expression.
//
//   5. exploreGroup: explores each expression in the group
//
//   6. exploreGroupExpr: explores all of the child groups (via exploreGroup),
//      then applies any applicable exploration transformations to the
//      expression.
//
//   7. transform: applies a transform to the forest of expressions rooted at a
//      particular group expression.

type search struct {
	memo *memo
}

func newSearch(memo *memo) *search {
	return &search{
		memo: memo,
	}
}

func (s *search) run() {
	s.implementGroup(s.memo.groups[s.memo.root])
}

func (s *search) optimizeGroup(g *memoGroup) {
	// Before optimizing a group, we need to implement it.
	s.implementGroup(g)

	if g == nil || g.state >= stateOptimizing {
		return
	}
	g.state = stateOptimizing

	// Optimize each expression in the group.
	for _, e := range g.exprs {
		s.optimizeGroupExpr(e)
	}

	g.state = stateImplemented
}

func (s *search) optimizeGroupExpr(e *memoExpr) {
	if e.state >= stateOptimizing {
		return
	}
	e.state = stateOptimizing

	// Optimize children groups first.
	for _, c := range e.children {
		s.optimizeGroup(s.memo.groups[c])
	}

	// TODO(peter): unimplemented.
	e.state = stateOptimized
}

func (s *search) implementGroup(g *memoGroup) {
	// Before implementing a group, we need to explore it.
	s.exploreGroup(g)

	if g == nil || g.state >= stateImplementing {
		return
	}
	g.state = stateImplementing

	// Implement each expression in the group.
	for _, e := range g.exprs {
		s.implementGroupExpr(e)
	}

	g.state = stateImplemented
}

func (s *search) implementGroupExpr(e *memoExpr) {
	if e.state >= stateImplementing {
		return
	}
	e.state = stateImplementing

	// Implement children groups first.
	for _, c := range e.children {
		s.implementGroup(s.memo.groups[c])
	}

	// Apply implementation transformations.
	for _, xid := range implementationXforms[e.op] {
		s.transform(e, xid)
	}

	e.state = stateImplemented
}

func (s *search) exploreGroup(g *memoGroup) {
	if g == nil || g.state >= stateExploring {
		return
	}
	g.state = stateExploring

	// Explore each expression in the group.
	for _, e := range g.exprs {
		s.exploreGroupExpr(e)
	}

	g.state = stateExplored
}

func (s *search) exploreGroupExpr(e *memoExpr) {
	if e.state >= stateExploring {
		return
	}
	e.state = stateExploring

	// Explore children groups first.
	for _, c := range e.children {
		s.exploreGroup(s.memo.groups[c])
	}

	// Apply exploration transformations.
	for _, xid := range explorationXforms[e.op] {
		s.transform(e, xid)
	}

	e.state = stateExplored
}

func (s *search) transform(e *memoExpr, xid xformID) {
	xform := xforms[xid]
	pattern := xform.pattern()
	var results []*expr

	for cursor := (*expr)(nil); ; {
		cursor = s.memo.bind(e, pattern, cursor)
		if cursor == nil {
			break
		}
		if !xform.check(cursor) {
			continue
		}
		results = xform.apply(cursor, results)
	}

	// TODO(peter): Adding the expressions back to the memo might create more
	// groups or add expressions to existing groups.
	for _, r := range results {
		// The group that the top-level expressions get added back to is required
		// to be the source group.
		r.loc = memoLoc{e.loc.group, -1}
		s.memo.addExpr(r)
	}
}
