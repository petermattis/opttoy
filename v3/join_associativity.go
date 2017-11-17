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
	return &expr{
		op: innerJoinOp,
		children: []*expr{
			&expr{ // left
				op: innerJoinOp,
				children: []*expr{
					patternLeaf, // left
					patternLeaf, // right
					patternTree, // filter
				},
			},
			patternLeaf, // right
			patternTree, // filter
		},
	}
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

	// Split the filters on the upper and lower joins into new sets.
	var lowerFilters []*expr
	var upperFilters []*expr
	var lowerCols bitmap
	lowerCols.UnionWith(leftLeft.props.outputCols)
	lowerCols.UnionWith(right.props.outputCols)
	for _, filters := range [2][]*expr{e.filters(), left.filters()} {
		for _, f := range filters {
			if f.scalarInputCols().SubsetOf(lowerCols) {
				lowerFilters = append(lowerFilters, f)
			} else {
				upperFilters = append(upperFilters, f)
			}
		}
	}

	if len(upperFilters) > 0 && len(lowerFilters) == 0 {
		// Don't create cross joins if the upper join has a filter.
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

	// Compute the output columns for the lower join.
	newLower.props.outputCols = newLower.props.outputCols.Intersection(
		newUpper.requiredInputCols().Union(newUpper.props.outputCols),
	)

	return append(results, newUpper)
}
