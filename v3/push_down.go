package v3

func substitute(e *expr, columns bitmap, replacement *expr) *expr {
	if e.op == variableOp && e.inputVars == columns {
		return replacement
	}

	result := e.clone()
	inputs := result.inputs()
	for i, input := range inputs {
		inputs[i] = substitute(input, columns, replacement)
	}
	result.updateProps()
	return result
}

func isFilterCompatible(e *expr, filter *expr) bool {
	// NB: when pushing down a filter, the filter applies before the projection
	// and thus needs to be compatible with the input variables, not the output
	// variables.
	return (filter.inputVars & e.inputVars) == filter.inputVars
}

func findEquivalency(filters []*expr, e *expr) *expr {
	for _, filter := range filters {
		if filter.op == eqOp {
			left := filter.inputs()[0]
			right := filter.inputs()[1]
			if left.op == variableOp && right.op == variableOp {
				if left.inputVars == e.inputVars {
					return right
				}
				if right.inputVars == e.inputVars {
					return left
				}
			}
		}
	}
	return nil
}

// TODO(peter): The current code is likely incorrect in various ways. Below is
// what we should be doing.
//
// We want to push filters through relational operators, but not onto
// relational operators. For example:
//
//   select a.x > 1    --->  join a, b
//     join                    select a.x > 1
//       scan a (x, y)           scan a (x, y)
//       scan b (x, z)         scan b (x, z)
//
// While pushing a filter down, we need to infer additional filters using
// column equivalencies.
//
//   select a.x > 1    ---> join a.x = b.x
//     join a.x = b.x         select a.x > 1
//       scan a (x, y)          scan a (x, y)
//       scan b (x, z)        select b.x > 1
//                              scan b (x, z)
//
// Note that a filter might be compatible with a relational operator, but not
// with its inputs. Consider:
//
//   select a.y + b.z > 1
//     join a.x = b.x
//       scan a (x, y)
//       scan b (x, z)
func pushDownFilters(e *expr) {
	// Push down filters to inputs.
	filters := e.filters()
	// Strip off all of the filters. We'll re-add any filters that couldn't be
	// pushed down.
	e.removeFilters()

	for _, filter := range filters {
		var count int
		for _, input := range e.inputs() {
			if isFilterCompatible(input, filter) {
				input.addFilter(filter)
				count++
				continue
			}

			// Check to see if creating a new filter by substitution could be pushed down.
			if replacement := findEquivalency(filters, filter); replacement != nil {
				if isFilterCompatible(input, replacement) {
					newFilter := substitute(filter, filter.inputVars, replacement)
					input.addFilter(newFilter)
					count++
					continue
				}
			}

			// Rewrite filters as they are pushed through projections.
			//
			// TODO(peter): doing something operator specific like this highlights
			// the need for an operator-specific interface for inferring predicates
			// from other predicates.
			if input.op == projectOp {
				for _, project := range input.projections() {
					if filter.inputVars == 1<<project.varIndex {
						newFilter := substitute(filter, filter.inputVars, project)
						input.addFilter(newFilter)
						count++
						continue
					}
				}
			}
		}
		if count == 0 {
			e.addFilter(filter)
		}
	}

	for _, input := range e.inputs() {
		input.updateProps()
		pushDownFilters(input)
	}

	e.updateProps()
}
