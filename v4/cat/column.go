package cat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type ColumnName string
type ColumnOrdinal int

type Column struct {
	Name    ColumnName
	NotNull bool
	Type    types.T
	Stats   *Histogram
}
