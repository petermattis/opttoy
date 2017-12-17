package v3

import "github.com/cockroachdb/cockroach/pkg/sql/sem/types"

type scalarProps struct {
	// Columns used by the scalar expression.
	inputCols bitmap

	typ types.T
}
