package v3

import (
	"math/bits"
)

// TODO(peter):
// - Normalize filers
func prep(e *expr) {
	inferFilters(e)
}

func inferFilters(e *expr) {
	for _, input := range e.inputs() {
		inferFilters(input)
	}

	// TODO(peter): infer IS NOT NULL filters.

	// For each equivalent column set, look for filters which use on one of the
	// equivalent columns. If such a filter, is found, create additional filters
	// via substitution.
	var inferredFilters []*expr
	for _, equiv := range e.props.equivCols {
		for _, filter := range e.filters() {
			filterInputCols := filter.scalarInputCols()
			if (filterInputCols & equiv) == 0 {
				continue
			}
			if filterInputCols.count() != 1 {
				continue
			}
			filterCol := bitmapIndex(bits.TrailingZeros64(uint64(filterInputCols)))
			// Loop over the equivalent columns and create new expressions by
			// substitution.
			for equiv != 0 {
				i := bitmapIndex(bits.TrailingZeros64(uint64(equiv)))
				equiv.clear(i)
				if filterCol == i {
					continue
				}
				// TODO(peter): We should be able to generate the replacement from
				// e.props, but natural and using joins currently don't output all of
				// their input columns which is a mistake.
				for _, input := range e.inputs() {
					if input.props.outputCols.get(i) {
						replacement := input.props.newColumnExprByIndex(i)
						newFilter := substitute(filter, filterInputCols, replacement)
						inferredFilters = append(inferredFilters, newFilter)
					}
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
