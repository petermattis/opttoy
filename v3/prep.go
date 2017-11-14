package v3

import (
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TODO(peter):
// - Column numbering pass
// - Normalize filters
func prep(e *expr) {
	trimOutputCols(e, e.props.outputCols)
	inferFilters(e)
	pushDownFilters(e)
	xformApplyAll(joinElimination{}, e)
}

// Push down required output columns from the root of the expression to leaves.
func trimOutputCols(e *expr, requiredOutputCols bitmap) {
	e.props.outputCols = requiredOutputCols
	requiredInputCols := e.requiredInputCols() | requiredOutputCols
	for _, input := range e.inputs() {
		trimOutputCols(input, requiredInputCols&input.props.outputCols)
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
		var count int
		for _, filter := range e.filters() {
			filterInputCols := filter.scalarInputCols()
			if (filterInputCols & equiv) == 0 {
				continue
			}
			if filterInputCols.count() != 1 {
				continue
			}
			count++
			filterCol := bitmapIndex(bits.TrailingZeros64(uint64(filterInputCols)))
			// Loop over the equivalent columns and create new expressions by
			// substitution.
			for v := equiv; v != 0; {
				i := bitmapIndex(bits.TrailingZeros64(uint64(v)))
				v.clear(i)
				if filterCol == i {
					continue
				}

				replacement := e.props.findColumnByIndex(i).newVariableExpr("")
				newFilter := substitute(filter, filterInputCols, replacement)
				inferredFilters = append(inferredFilters, newFilter)
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
		inputNotNullCols.unionWith(input.props.notNullCols)
	}
	newNotNullCols := e.props.notNullCols & ^inputNotNullCols

	// Only infer filters for required output columns.
	newNotNullCols &= (e.props.outputCols | e.requiredFilterCols())

	// Remove any columns for which a filter already exists on only that column
	// which filters NULLs.
	for _, filter := range e.filters() {
		filterInputCols := filter.scalarInputCols()
		if filterInputCols.count() != 1 {
			continue
		}
		filterCol := bitmapIndex(bits.TrailingZeros64(uint64(filterInputCols)))
		newNotNullCols.clear(filterCol)
	}

	// Generate the IS NOT NULL filters for the remaining columns.
	for v := newNotNullCols; v != 0; {
		i := bitmapIndex(bits.TrailingZeros64(uint64(v)))
		v.clear(i)

		newFilter := newBinaryExpr(isNotOp,
			e.props.findColumnByIndex(i).newVariableExpr(""),
			newConstExpr(tree.DNull))
		newFilter.updateProps()
		e.addFilter(newFilter)
	}
}
