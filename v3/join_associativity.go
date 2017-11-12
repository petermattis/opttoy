package v3

func init() {
	registerXform(joinAssociativity{})
}

type joinAssociativity struct {
	xformExploration
}

func (joinAssociativity) id() xformID {
	return xformJoinAssociativityID
}

func (joinAssociativity) pattern() *expr {
	return newJoinPattern(innerJoinOp,
		newJoinPattern(innerJoinOp,
			nil /* left */, nil /* right */, patternTree /* filter */), /* left */
		nil, /* right */
		patternTree /* filter */)
}

func (joinAssociativity) check(e *expr) bool {
	return true
}

// (RS)T -> (RT)S
func (joinAssociativity) apply(e *expr, results []*expr) []*expr {
	left := e.children[0]
	leftLeft := left.children[0]
	leftRight := left.children[1]
	right := e.children[1]

	// TODO(peter): Imagine the query:
	//
	//   a, b, c WHERE a.x = b.y AND b.y = c.z
	//
	// In order to create the expression `a JOIN c` we need to infer the filter
	// `a.x = c.z`. The creation of inferred expressions should happen as a prep
	// pass.

	// Split the filters on the upper and lower joins into new sets.
	var lowerFilters []*expr
	var upperFilters []*expr
	lowerVars := leftLeft.props.outputVars | right.props.outputVars
	for _, filters := range [2][]*expr{e.filters(), left.filters()} {
		for _, f := range filters {
			if (lowerVars & f.scalarInputVars()) == f.scalarInputVars() {
				lowerFilters = append(lowerFilters, f)
			} else {
				upperFilters = append(upperFilters, f)
			}
		}
	}

	if len(lowerFilters) == 0 {
		// Don't create cross joins.
		return results
	}

	newLower := newJoinExpr(innerJoinOp, leftLeft, right)
	newLower.addFilters(lowerFilters)
	newLower.props = &relationalProps{
		columns: make([]columnProps, len(leftLeft.props.columns)+len(right.props.columns)),
	}
	copy(newLower.props.columns[:], leftLeft.props.columns)
	copy(newLower.props.columns[len(leftLeft.props.columns):], right.props.columns)
	newLower.initProps()

	newUpper := newJoinExpr(innerJoinOp, newLower, leftRight)
	newUpper.addFilters(upperFilters)
	newUpper.props = e.props
	return append(results, newUpper)
}
