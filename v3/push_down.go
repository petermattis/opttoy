package v3

// Push down filters through relational operators. For example:
//
//   select a.x > 1    --->  join a, b
//     join                    select a.x > 1
//       scan a (x, y)           scan a (x, y)
//       scan b (x, z)         scan b (x, z)
//
// Note that a filter might be compatible with a relational operator, but not
// with its inputs. Consider:
//
//   select a.y + b.z > 1
//     join a.x = b.x
//       scan a (x, y)
//       scan b (x, z)
//
// The general strategy is to walk down the expression tree looking for
// filters. For each filter, we try to push it down either on to or below the
// input for the relational expression containing the filter. Because filters
// are only present on selects and joins, pushing a filter below a relational
// expression might require construction of a new select expression.
func pushDownFilters(e *expr) {
	if e.op == selectOp {
		switch e.children[0].op {
		case groupByOp:
			pushDownFiltersSelectGroupBy(e)
		case innerJoinOp:
			pushDownFiltersSelectInnerJoin(e)
		case projectOp:
			pushDownFiltersSelectProject(e)
		case unionOp:
			pushDownFiltersSelectUnion(e)
		}

		// We possibly pushed a filter down below the select input. Update the
		// output columns for the input.
		requiredInputCols := e.requiredInputCols().Union(e.props.outputCols)
		for _, input := range e.inputs() {
			input.props.outputCols = requiredInputCols.Intersection(input.props.availableOutputCols())
		}
		e.updateProps()

		// Elide the select expression if there are no more filters.
		if len(e.filters()) == 0 {
			*e = *e.children[0]
			pushDownFilters(e)
			return
		}
	} else if e.op == innerJoinOp {
		pushDownFiltersJoin(e)
	}

	for _, input := range e.inputs() {
		pushDownFilters(input)
	}
}

// Push a filter onto e. If e does not accept filters, replace it with a
// selectOp.
func pushFilter(e, filter *expr) {
	if e.layout().filters < 0 {
		t := *e
		*e = expr{
			op:       selectOp,
			children: []*expr{&t, nil /* filter */},
			props: &relationalProps{
				columns: make([]columnProps, len(e.props.columns)),
			},
		}
		copy(e.props.columns, t.props.columns)
		e.props.outputCols = t.props.outputCols.Copy()
	}
	e.addFilter(filter)
}

func pushDownFiltersSelectGroupBy(e *expr) {
	// TODO(peter): unimplemented
}

func pushDownFiltersSelectInnerJoin(e *expr) {
	filters := e.filters()
	newFilters := filters[:0]
	input := e.children[0]

	for _, filter := range e.filters() {
		// First try to push the filter below the join.
		var count int
		for _, joinInput := range input.inputs() {
			if filter.scalarInputCols().SubsetOf(joinInput.props.availableOutputCols()) {
				pushFilter(joinInput, filter)
				joinInput.updateProps()
				count++
			}
		}

		if count == 0 {
			// If we couldn't push the filter below the join, try to push it onto the
			// join.
			if filter.scalarInputCols().SubsetOf(input.props.availableOutputCols()) {
				pushFilter(input, filter)
				count++
				continue
			}

			newFilters = append(newFilters, filter)
		}
	}

	e.replaceFilters(newFilters)
	input.updateProps()
	e.updateProps()
}

func pushDownFiltersSelectProject(e *expr) {
	filters := e.filters()
	newFilters := filters[:0]
	input := e.children[0]
	projectInput := input.children[0]

	for _, filter := range e.filters() {
		// First try to push the filter below the projection.
		var count int
		if filter.scalarInputCols().SubsetOf(projectInput.props.availableOutputCols()) {
			pushFilter(projectInput, filter)
			count++
			continue
		}

		// Failed to push the filter as-is, so try to create a new filter using one
		// of the projection expressions.
		for i, project := range input.projections() {
			// The order of the projections maps precisely to the order of the output
			// columns.
			col := &input.props.columns[i]
			if filter.scalarInputCols().Contains(col.index) {
				newFilter := substitute(filter, filter.scalarInputCols(), project)
				if newFilter.scalarInputCols().SubsetOf(projectInput.props.availableOutputCols()) {
					pushFilter(projectInput, newFilter)
					count++
					break
				}
			}
		}

		if count == 0 {
			newFilters = append(newFilters, filter)
		}
	}

	e.replaceFilters(newFilters)
	projectInput.updateProps()
	e.updateProps()
}

func pushDownFiltersSelectUnion(e *expr) {
	// TODO(peter): unimplemented
}

func pushDownFiltersJoin(e *expr) {
	filters := e.filters()
	newFilters := filters[:0]

	for _, filter := range e.filters() {
		var count int
		for _, input := range e.inputs() {
			if filter.scalarInputCols().SubsetOf(input.props.availableOutputCols()) {
				pushFilter(input, filter)
				count++
			}
		}

		if count == 0 {
			newFilters = append(newFilters, filter)
		}
	}

	e.replaceFilters(newFilters)
	for _, input := range e.inputs() {
		input.updateProps()
	}
	e.updateProps()
}
