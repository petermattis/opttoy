package v3

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type bucket struct {
	// The number of values in the bucket.
	numRange int64
	// The upper boundary of the bucket. The column values for the upper bound
	// are encoded using the ascending key encoding.
	upperBound tree.Datum
}

type histogram struct {
	// The total number of rows in the table.
	rowCount int64
	// The estimated cardinality (distinct values) for the column.
	distinctCount int64
	// The number of NULL values for the column.
	nullCount int64
	// The histogram buckets which describe the distribution of non-NULL
	// values. The buckets are sorted by bucket.upperBound.
	buckets []bucket
}

func (h *histogram) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "rows:     %d\n", h.rowCount)
	fmt.Fprintf(&buf, "distinct: %d\n", h.distinctCount)
	fmt.Fprintf(&buf, "nulls:    %d\n", h.nullCount)
	fmt.Fprintf(&buf, "buckets: ")
	for _, b := range h.buckets {
		fmt.Fprintf(&buf, " %s:%d", b.upperBound, b.numRange)
	}
	buf.WriteString("\n")
	return buf.String()
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
func createHistogram(
	catalog map[tableName]*table,
	tblName tableName, colName columnName,
	rows *tree.Select,
) *histogram {
	values, ok := rows.Select.(*tree.ValuesClause)
	if !ok {
		fatalf("unsupported rows: %s", rows)
	}

	tab, ok := catalog[tblName]
	if !ok {
		fatalf("unable to find table %s", tblName)
	}

	colIdx, ok := tab.colMap[colName]
	if !ok {
		fatalf("unable to find %s.%s", tblName, colName)
	}

	col := &tab.columns[colIdx]
	col.hist = &histogram{}

	for _, v := range values.Tuples {
		if len(v.Exprs) != 2 {
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
			col.hist.buckets = append(col.hist.buckets, bucket{
				numRange:   val,
				upperBound: upperBound,
			})
		case *tree.StrVal:
			switch t.RawString() {
			case "rows":
				col.hist.rowCount = val
			case "distinct":
				col.hist.distinctCount = val
			case "nulls":
				col.hist.nullCount = val
			}
		default:
			unimplemented("histogram bucket: %T", v.Exprs[0])
		}

	}

	sort.Slice(col.hist.buckets, func(i, j int) bool {
		bi := &col.hist.buckets[i]
		bj := &col.hist.buckets[j]
		return bi.upperBound.Compare(nil, bj.upperBound) < 0
	})

	return col.hist
}
