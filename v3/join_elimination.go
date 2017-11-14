package v3

func joinElimination(e *expr) {
	if e.op == innerJoinOp {
		inputs := e.inputs()
		left := inputs[0]
		right := inputs[1]
		// Try to eliminate the right side of the join. Because inner join is
		// symmetric, we can use the same code to try and eliminate the left side
		// of the join.
		if !maybeEliminateInnerJoin(e, left, right) {
			maybeEliminateInnerJoin(e, right, left)
		}
	}
	for _, input := range e.inputs() {
		joinElimination(input)
	}
	e.updateProps()
}

// Check to see if the right side of the join is unnecessary.
func maybeEliminateInnerJoin(e, left, right *expr) bool {
	// Check to see if the required output columns only depend on the left side
	// of the join.
	leftOutputCols := left.props.outputCols
	if !e.props.outputCols.subsetOf(leftOutputCols) {
		return false
	}

	// Look for a foreign key in the left side of the join which maps to a unique
	// index on the right side of the join.
	rightOutputCols := right.props.outputCols
	var fkey *foreignKeyProps
	for i := range left.props.foreignKeys {
		fkey = &left.props.foreignKeys[i]
		if !fkey.src.subsetOf(left.props.notNullCols) {
			// The source for the foreign key is a weak key.
			continue
		}
		if fkey.dest.subsetOf(rightOutputCols) {
			// The target of the foreign key is the right side of the join.
			break
		}
		fkey = nil
	}
	if fkey == nil {
		return false
	}

	// Make sure any filters present other than the join condition only apply to
	// the left hand side of the join.
	filters := e.filters()
	for _, filter := range filters {
		// TODO(peter): pushDownFilters() should ensure we only have join
		// conditions here making this test and the one for the left output columns
		// unnecessary.
		if filter.scalarInputCols().subsetOf(rightOutputCols) {
			// The filter only utilizes columns from the right hand side of the join.
			return false
		}
		if filter.scalarInputCols().subsetOf(leftOutputCols) {
			// The filter only utilizes columns from the left hand side of the join.
			continue
		}
		// TODO(peter): how to check for the join conditions? We need the join
		// condition to match the foreign key. This is easy for simple "a.x = b.x"
		// style join conditions. But what about "a.x = b.x AND a.y = b.y"? And
		// "(a.x, a.y) = (b.x, b.y)".
	}

	// Move any filters down to the left hand side of the join.
	for _, filter := range filters {
		if filter.scalarInputCols().subsetOf(leftOutputCols) {
			left.addFilter(filter)
		}
	}
	left.props.applyFilters(left.filters())

	*e = *left
	return true
}
