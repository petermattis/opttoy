package v3

func substitute(e *expr, columns bitmap, replacement *expr) *expr {
	if e.op == variableOp && e.outputVars == columns {
		return replacement
	}

	result := *e
	result.children = append([]*expr(nil), e.children...)

	for i, input := range result.inputs() {
		result.children[i] = substitute(input, columns, replacement)
	}
	result.updateProperties()
	return &result
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
		count := 0
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
		}
		if count == 0 {
			e.addFilter(filter)
		}
	}

	for _, input := range e.inputs() {
		input.updateProperties()
		pushDownFilters(input)
	}

	// TODO(peter): This is hacky and should be generalized. If filters were
	// added to a scanOp, lift the filters into a selectOp.
	if e.op == scanOp && len(e.filters()) > 0 {
		filters := e.filters()
		t := *e
		t.removeFilters()
		*e = expr{
			op:          selectOp,
			children:    make([]*expr, len(filters)+1),
			filterCount: int16(len(filters)),
		}
		e.inputs()[0] = &t
		copy(e.children[1:], filters)
		t.updateProperties()
	}
	if e.op == selectOp && len(e.filters()) == 0 {
		*e = *e.inputs()[0]
	}

	e.updateProperties()
}
