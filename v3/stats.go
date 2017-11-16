package v3

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type bucket struct {
	upperBound tree.Datum
	count      int64
}

type histogram struct {
	buckets []bucket
}

func (h *histogram) String() string {
	var buf bytes.Buffer
	for i, b := range h.buckets {
		if i > 0 {
			buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%s:%d", b.upperBound, b.count)
	}
	return buf.String()
}

// Create a histogram from an INSERT clause. The rows are expected to be a
// VALUES clause containing pairs of (upper-bound, count). Currently only INT
// or NULL constants are valid for upper-bound, and only INT constants are
// valid for count.
func createHistogram(
	catalog map[string]*table,
	tableName, colName string,
	rows *tree.Select,
) *histogram {
	values, ok := rows.Select.(*tree.ValuesClause)
	if !ok {
		fatalf("unsupported rows: %s", rows)
	}

	tab, ok := catalog[tableName]
	if !ok {
		fatalf("unable to find table %s", tableName)
	}

	colIdx, ok := tab.colMap[colName]
	if !ok {
		fatalf("unable to find %s.%s", tableName, colName)
	}

	col := &tab.columns[colIdx]
	col.hist = &histogram{
		buckets: make([]bucket, len(values.Tuples)),
	}

	for i, v := range values.Tuples {
		if len(v.Exprs) != 2 {
			fatalf("malformed histogram bucket: %s", v)
		}

		var err error
		if col.hist.buckets[i].count, err = v.Exprs[1].(*tree.NumVal).AsInt64(); err != nil {
			fatalf("malformed histogram bucket: %s: %v", v, err)
		}

		if v.Exprs[0] == tree.DNull {
			col.hist.buckets[i].upperBound = tree.DNull
			continue
		}

		switch t := v.Exprs[0].(type) {
		case *tree.NumVal:
			col.hist.buckets[i].upperBound, err = t.ResolveAsType(nil, types.Int)
			if err != nil {
				fatalf("malformed histogram bucket: %s: %v", v, err)
			}
		// TODO(peter): case *tree.StrVal:
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
