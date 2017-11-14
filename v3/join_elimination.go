package v3

func init() {
	registerXform(joinElimination{})
}

type joinElimination struct {
	xformExploration
}

func (joinElimination) id() xformID {
	return xformJoinEliminationID
}

func (joinElimination) pattern() *expr {
	return &expr{ /* left */
		op: innerJoinOp,
		children: []*expr{
			patternLeaf, /* left */
			patternLeaf, /* right */
			patternTree, /* filter */
		},
	}
}

func (joinElimination) check(e *expr) bool {
	return true
}

func (joinElimination) apply(e *expr, results []*expr) []*expr {
	// Try to eliminate the right side of the join. Because inner join is
	// symmetric, we can use the same code to try and eliminate the left side
	// of the join.
	left := e.children[0]
	right := e.children[1]
	if result := maybeEliminateInnerJoin(e, left, right); result != nil {
		return append(results, result)
	}
	if result := maybeEliminateInnerJoin(e, right, left); result != nil {
		return append(results, result)
	}
	return results
}

// Check to see if the right side of the join is unnecessary.
func maybeEliminateInnerJoin(e, left, right *expr) *expr {
	// Check to see if the required output columns only depend on the left side
	// of the join.
	leftOutputCols := left.props.outputCols
	if !e.props.outputCols.subsetOf(leftOutputCols) {
		return nil
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
		return nil
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
			return nil
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
	left.updateProps()
	return left
}
