package v3

// TODO(peter):
//
// scan a
//   exists
//     select (a.x = b.x)
//       scan b
//
// semi-join
//   scan a
//   select (a.x = b.x)
//     scan b
//
// select (a.x = b.x)
//   semi-join
//     scan a
//     scan b

func maybeExpandExists(e *expr, filter, filterTop *expr) bool {
	if filter.op != existsOp {
		return false
	}

	t := *e
	t.removeFilter(filterTop)

	subquery := filter.inputs()[0]
	if subquery.op == projectOp {
		// Projections can be stripped from the right hand side of a
		// {anti,semi}-join because all we care about is existence.
		*subquery = *subquery.inputs()[0]
	}

	*e = expr{
		op: semiJoinOp,
		children: []*expr{
			&t,
			subquery,
		},
		props: t.props,
	}
	e.setApply()
	e.updateProps()
	return true
}

func maybeExpandNotExists(e *expr, filter *expr) bool {
	if filter.op != notOp {
		return false
	}

	if !maybeExpandExists(e, filter.inputs()[0], filter) {
		return false
	}

	e.op = antiJoinOp
	return true
}

func maybeExpandJoin(e *expr) {
	if e.op == innerJoinOp {
		right := e.inputs()[1]
		if right.inputVars != 0 {
			e.setApply()
		}
	}
}

func maybeExpandApply(e *expr) bool {
	for _, filter := range e.filters() {
		if maybeExpandExists(e, filter, filter) {
			return true
		}
		if maybeExpandNotExists(e, filter) {
			return true
		}
	}

	maybeExpandJoin(e)
	return false
}

// apply(R, select(E)) -> select(apply(R, E))
func maybeDecorrelateSelection(e *expr) bool {
	right := e.inputs()[1]
	for _, filter := range right.filters() {
		if (filter.inputVars & e.props.outputVars()) != 0 {
			right.removeFilter(filter)
			right.updateProps()
			e.addFilter(filter)
			e.updateProps()
			return true
		}
	}
	return false
}

// apply(R, project(E)) -> project(apply(R, E))
func maybeDecorrelateProjection(e *expr) bool {
	// TODO(peter): unimplemented
	return false
}

// apply(R, join(A, B)) -> join(apply(R, A), apply(R, B))
func maybeDecorrelateJoin(e *expr) bool {
	// TODO(peter): unimplemented
	return false
}

// apply(R, groupBy(E)) -> groupBy(applyLOJ(R, E))
func maybeDecorrelateScalarGroupBy(e *expr) bool {
	// TODO(peter): unimplemented
	return false
}

// apply(R, groupBy(E)) -> groupBy(apply(R, E))
func maybeDecorrelateVectorGroupBy(e *expr) bool {
	// TODO(peter): unimplemented
	return false
}

func maybeDecorrelate(e *expr) bool {
	if !e.hasApply() {
		return false
	}

	if maybeDecorrelateSelection(e) {
		return true
	}
	if maybeDecorrelateProjection(e) {
		return true
	}

	e.clearApply()
	return false
}

func decorrelate(e *expr) {
	for maybeExpandApply(e) {
	}
	for maybeDecorrelate(e) {
	}

	for _, input := range e.inputs() {
		decorrelate(input)
	}
}
