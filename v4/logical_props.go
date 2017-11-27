package v4

type logicalProps struct {
	scalar scalarProps
}

type scalarProps struct {
	// Columns used directly or indirectly by the scalar expression.
	inputCols bitmap
}
