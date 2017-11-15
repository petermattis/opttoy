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

	// Trim relationalProps.columns to contain only required columns
	// (i.e. columns required by our parent expression or by this expression's
	// filters).
	requiredCols := e.props.outputCols
	requiredCols.unionWith(e.requiredFilterCols())

	columns := e.props.columns
	e.props.columns = e.props.columns[:0]
	for _, col := range columns {
		if requiredCols.get(col.index) {
			e.props.columns = append(e.props.columns, col)
		}
	}

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
		for _, filter := range e.filters() {
			filterInputCols := filter.scalarInputCols()
			if filterInputCols == 0 {
				// The filter doesn't have any input columns.
				continue
			}
			if filterInputCols == equiv {
				// The filter input columns are exactly the equivalent columns. No
				// substitutions are possible.
				continue
			}
			if (filterInputCols & equiv) == 0 {
				// The filter input columns do not overlap the equivalent columns. No
				// substitutiosn are possible.
				continue
			}

			// Loop over the equivalent columns and create new expressions by
			// substitution.
			for v := equiv & ^filterInputCols; v != 0; {
				i := bitmapIndex(bits.TrailingZeros64(uint64(v)))
				v.clear(i)
				col := e.props.findColumnByIndex(i)
				if col == nil {
					// The equivalent column is not needed by the expression, so don't
					// create a filter using it.
					continue
				}
				replacement := col.newVariableExpr("")

				for u := filterInputCols; u != 0; {
					j := bitmapIndex(bits.TrailingZeros64(uint64(u)))
					u.clear(j)
					if i == j {
						continue
					}
					var t bitmap
					t.set(j)
					newFilter := substitute(filter, t, replacement)
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
