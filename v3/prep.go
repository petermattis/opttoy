package v3

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func prep(e *expr) {
	trimOutputCols(e, e.props.outputCols)
	normalize(e)
	inferFilters(e)
	pushDownFilters(e)
	xformApplyAll(joinElimination{}, e)
}

// Push down required output columns from the root of the expression to leaves.
func trimOutputCols(e *expr, requiredOutputCols bitmap) {
	e.props.outputCols = requiredOutputCols
	requiredInputCols := e.requiredInputCols().Union(requiredOutputCols)
	for _, input := range e.inputs() {
		trimOutputCols(input, requiredInputCols.Intersection(input.props.outputCols))
	}
	e.updateProps()
}

func inferFilters(e *expr) {
	for _, input := range e.inputs() {
		inferFilters(input)
	}

	inferEquivFilters(e)
	inferNotNullFilters(e)
}

func inferEquivFilters(e *expr) {
	// For each equivalent column set, look for filters which use on one of the
	// equivalent columns. If such a filter, is found, create additional filters
	// via substitution.
	var inferredFilters []*expr
	for _, equiv := range e.props.equivCols {
		for _, filter := range e.filters() {
			filterInputCols := filter.scalarInputCols()
			if filterInputCols.Empty() {
				// The filter doesn't have any input columns.
				continue
			}
			if filterInputCols == equiv {
				// The filter input columns are exactly the equivalent columns. No
				// substitutions are possible.
				continue
			}
			if !filterInputCols.Intersects(equiv) {
				// The filter input columns do not overlap the equivalent columns. No
				// substitutiosn are possible.
				continue
			}

			// Loop over the equivalent columns and create new expressions by
			// substitution.
			v := equiv.Difference(filterInputCols)
			for i, ok := v.Next(0); ok; i, ok = v.Next(i + 1) {
				replacement := e.props.findColumnByIndex(i).newVariableExpr("")

				for j, ok := filterInputCols.Next(0); ok; j, ok = filterInputCols.Next(j + 1) {
					var t bitmap
					t.Add(j)
					newFilter := substitute(filter, t, replacement)
					normalize(newFilter)
					inferredFilters = append(inferredFilters, newFilter)
				}
			}
		}
	}

	// Only add inferred filters if they don't already exist.
	for _, inferred := range inferredFilters {
		var exists bool
		for _, filter := range e.filters() {
			exists = filter.equal(inferred)
			if exists {
				break
			}
		}
		if !exists {
			e.addFilter(inferred)
		}
	}
}

func inferNotNullFilters(e *expr) {
	// Determine which columns became NOT NULL at this expression (i.e. they were
	// nullable in the inputs).
	var inputNotNullCols bitmap
	for _, input := range e.inputs() {
		inputNotNullCols.UnionWith(input.props.notNullCols)
	}
	newNotNullCols := e.props.notNullCols.Difference(inputNotNullCols)

	// Only infer filters for required output columns.
	newNotNullCols.IntersectionWith(e.props.outputCols.Union(e.requiredFilterCols()))

	// Remove any columns for which a filter already exists on only that column
	// which filters NULLs.
	for _, filter := range e.filters() {
		filterInputCols := filter.scalarInputCols()
		if filterInputCols.Len() != 1 {
			continue
		}
		newNotNullCols.DifferenceWith(filterInputCols)
	}

	// Generate the IS NOT NULL filters for the remaining columns.
	for i, ok := newNotNullCols.Next(0); ok; i, ok = newNotNullCols.Next(i + 1) {
		newFilter := newBinaryExpr(
			isNotOp,
			e.props.findColumnByIndex(i).newVariableExpr(""),
			newConstExpr(tree.DNull),
		)
		newFilter.updateProps()
		e.addFilter(newFilter)
	}
}
