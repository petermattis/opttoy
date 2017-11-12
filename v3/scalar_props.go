package v3

type scalarProps struct {
	// Variables defined by the scalar expression.
	definedVars bitmap

	// Variables used by the scalar expression.
	inputVars bitmap

	// Does the scalar expression contain a subquery.
	containsSubquery bool
}
