package v3

type scalarProps struct {
	// Columns defined by the scalar expression.
	definedCols bitmap

	// Columns used by the scalar expression.
	inputCols bitmap

	// Does the scalar expression contain a subquery.
	containsSubquery bool
}
