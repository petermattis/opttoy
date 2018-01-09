package exec

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/petermattis/opttoy/v4/cat"
)

type createHistogram struct {
	catalog *cat.Catalog
}

// Create a histogram from an INSERT clause. The rows are expected to be a
// VALUES clause containing pairs of (upper-bound, count). Currently only INT
// or NULL constants are valid for upper-bound, and only INT constants are
// valid for count.
//
// Row, distinct and null counts can be specified by using the strings 'rows',
// 'distinct' and 'nulls' respectively for the upper-bound. For example:
//
//   VALUES ('rows', 1000), ('distinct', 100), ('nulls', 10)
//
// This creates a histogram with rowCount=1000, distinctCount=100, nullCount=10
// and no buckets.
func (ch *createHistogram) execute(tblName cat.TableName, colName cat.ColumnName, rows *tree.Select) *cat.Histogram {
	values, ok := rows.Select.(*tree.ValuesClause)
	if !ok {
		fatalf("unsupported rows: %s", rows)
	}

	tbl := ch.catalog.Table(tblName)
	col := tbl.Column(colName)
	hist := &cat.Histogram{}

	for _, v := range values.Tuples {
		if len(v.Exprs) != 2 && len(v.Exprs) != 3 {
			fatalf("malformed histogram bucket: %s", v)
		}

		val, err := v.Exprs[1].(*tree.NumVal).AsInt64()
		if err != nil {
			fatalf("malformed histogram bucket: %s: %v", v, err)
		}

		switch t := v.Exprs[0].(type) {
		case *tree.NumVal:
			upperBound, err := t.ResolveAsType(nil, types.Int)
			if err != nil {
				fatalf("malformed histogram bucket: %s: %v", v, err)
			}

			// Buckets have 3 values.
			if len(v.Exprs) != 3 {
				fatalf("malformed histogram bucket: %s", v)
			}

			numEq, err := v.Exprs[2].(*tree.NumVal).AsInt64()
			if err != nil {
				fatalf("malformed histogram bucket: %s: %v", v, err)
			}

			hist.Buckets = append(hist.Buckets, cat.Bucket{NumEq: numEq, NumRange: val, UpperBound: upperBound})

		case *tree.StrVal:
			switch t.RawString() {
			case "rows":
				hist.RowCount = val
			case "distinct":
				hist.DistinctCount = val
			case "nulls":
				hist.NullCount = val
			}

		default:
			unimplemented("histogram bucket: %T", v.Exprs[0])
		}

	}

	sort.Slice(hist.Buckets, func(i, j int) bool {
		bi := &hist.Buckets[i]
		bj := &hist.Buckets[j]
		return bi.UpperBound.Compare(nil, bj.UpperBound) < 0
	})

	hist.Validate()

	// Update the table column's stats with the new histogram.
	col.Stats = hist

	return hist
}
