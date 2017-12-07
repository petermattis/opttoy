package v3

import (
	"bytes"
	"fmt"
	"sort"

	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type bucket struct {
	// The number of values in the bucket equal to upperBound.
	numEq int64
	// The number of values in the bucket, excluding those that are
	// equal to upperBound.
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
	// One less than the minimum value of the column.
	lowerBound tree.Datum
	// The histogram buckets which describe the distribution of non-NULL
	// values. The buckets are sorted by bucket.upperBound.
	buckets []bucket
}

func (h *histogram) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "rows:       %d\n", h.rowCount)
	fmt.Fprintf(&buf, "distinct:   %d\n", h.distinctCount)
	fmt.Fprintf(&buf, "nulls:      %d\n", h.nullCount)
	fmt.Fprintf(&buf, "lowerBound: %s\n", h.lowerBound)
	fmt.Fprintf(&buf, "buckets:   ")
	for _, b := range h.buckets {
		fmt.Fprintf(&buf, " %s:%d,%d", b.upperBound, b.numRange, b.numEq)
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

			col.hist.buckets = append(col.hist.buckets, bucket{
				numEq:      numEq,
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
			case "lowerBound":
				col.hist.lowerBound, err = v.Exprs[1].(*tree.NumVal).ResolveAsType(nil, types.Int)
				if err != nil {
					fatalf("malformed histogram bucket: %s: %v", v, err)
				}
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

// filterHistogram filters a histogram based on the WHERE clause in the given select
// statement, and returns the histogram.  It expects a statement of the form:
//   SELECT * from histogram.table.column WHERE ...
// Currently the only operators supported in the WHERE clause are <, <=, >, and >=.
func filterHistogram(catalog map[tableName]*table, stmt *tree.Select) *histogram {
	sel, ok := stmt.Select.(*tree.SelectClause)
	if !ok {
		unimplemented("%s", stmt)
	}

	// Get the histogram name.
	name, ok := sel.From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.NormalizableTableName)
	if !ok {
		unimplemented("%s", stmt)
	}
	tname, err := name.Normalize()
	if err != nil {
		fatalf("unable to normalize: %v", err)
	}
	if tname.PrefixName != "histogram" {
		unimplemented("%s", stmt)
	}

	// Get the histogram from the catalog.
	tblName, colName := tableName(tname.DatabaseName), columnName(tname.TableName)
	tab, ok := catalog[tblName]
	if !ok {
		fatalf("unable to find table %s", tblName)
	}
	colIdx, ok := tab.colMap[colName]
	if !ok {
		fatalf("unable to find %s.%s", tblName, colName)
	}
	hist := tab.columns[colIdx].hist

	// Filter the histogram.
	expr, ok := sel.Where.Expr.(*tree.ComparisonExpr)
	if !ok {
		unimplemented("%s", stmt)
	}
	op := comparisonOpMap[expr.Operator]
	val, err := expr.Right.(*tree.NumVal).AsInt64()
	if err != nil {
		unimplemented("%s", stmt)
	}
	switch op {
	case leOp, ltOp:
		return hist.filterHistogramLtOpLeOp(op, val)
	case geOp, gtOp:
		return hist.filterHistogramGtOpGeOp(op, val)
	default:
		unimplemented("%s", stmt)
	}

	return nil
}

// newHistogram creates a new histogram given new buckets and a new lower bound,
// which represent a filtered version of the existing histogram h.
func (h *histogram) newHistogram(newBuckets []bucket, lowerBound tree.Datum) *histogram {
	total := int64(0)
	for _, b := range newBuckets {
		total += b.numEq + b.numRange
	}

	selectivity := float64(total) / float64(h.rowCount)
	return &histogram{
		rowCount: total,

		// Estimate the new distinctCount based on the selectivity of this filter.
		// todo(rytaft): this could be more precise if we take into account the
		// null count of the original histogram.
		distinctCount: int64(float64(h.distinctCount) * selectivity),

		// All the returned rows will be non-null for this column.
		nullCount:  0,
		lowerBound: lowerBound,
		buckets:    newBuckets,
	}
}

// filterHistogramLtOpLeOp applies a filter to the histogram that compares
// the histogram column value to a constant value with a ltOp or leOp (e.g., x < 4).
// Returns an updated histogram including only the values that satisfy the predicate.
func (h *histogram) filterHistogramLtOpLeOp(op operator, val int64) *histogram {
	if op != ltOp && op != leOp {
		panic("filterHistogramLtOpLeOp called with operator " + op.String())
	}

	// NB: The following logic only works for integer valued columns. This will need to
	// be altered for floating point columns and other types.
	lowerBound := (int64)(*h.lowerBound.(*tree.DInt))
	var newBuckets []bucket
	for _, b := range h.buckets {
		if val <= lowerBound {
			break
		}
		upperBound := (int64)(*b.upperBound.(*tree.DInt))
		if val < upperBound {
			// The bucket size calculation has a -1 because numRange does not
			// include values equal to upperBound.
			bucketSize := upperBound - lowerBound - 1
			if bucketSize > 0 {
				var bucketMatchSize int64
				var newUpperBound int64
				if op == leOp {
					// The matching values include val for leOp.
					bucketMatchSize = val - lowerBound
					newUpperBound = val + 1
				} else { /* op == ltOp */
					// The matching values do not include val for ltOp.
					bucketMatchSize = val - lowerBound - 1
					newUpperBound = val
				}

				// Create the new bucket.
				buc := bucket{
					numEq: 0,
					// Assuming a uniform distribution.
					numRange: (int64)(float64(b.numRange) * float64(bucketMatchSize) / float64(bucketSize)),
				}
				v := &tree.NumVal{Value: constant.MakeInt64(newUpperBound)}
				var err error
				buc.upperBound, err = v.ResolveAsType(nil, types.Int)
				if err != nil {
					fatalf("malformed histogram bucket: %s: %v", v, err)
				}
				newBuckets = append(newBuckets, buc)
			}
			break
		}
		if val == upperBound {
			buc := b
			if op == ltOp {
				buc.numEq = 0
			}
			newBuckets = append(newBuckets, buc)
			break
		}

		newBuckets = append(newBuckets, b)
		lowerBound = upperBound
	}

	return h.newHistogram(newBuckets, h.lowerBound)
}

// filterHistogramGtOpGeOp applies a filter to the histogram that compares
// the histogram column value to a constant value with a gtOp or geOp (e.g., x > 4).
// Returns an updated histogram including only the values that satisfy the predicate.
func (h *histogram) filterHistogramGtOpGeOp(op operator, val int64) *histogram {
	if op != gtOp && op != geOp {
		panic("filterHistogramGtOpGeOp called with operator " + op.String())
	}

	// NB: The following logic only works for integer valued columns. This will need to
	// be altered for floating point columns and other types.
	var newBuckets []bucket
	for i := len(h.buckets) - 1; i >= 0; i-- {
		b := h.buckets[i]
		upperBound := (int64)(*b.upperBound.(*tree.DInt))
		if val > upperBound {
			break
		}
		if val == upperBound {
			if op == geOp {
				buc := b
				buc.numRange = 0
				newBuckets = append(newBuckets, buc)
			}
			break
		}

		var lowerBound int64
		if i == 0 {
			lowerBound = (int64)(*h.lowerBound.(*tree.DInt))
		} else {
			lowerBound = (int64)(*h.buckets[i-1].upperBound.(*tree.DInt))
		}
		if val > lowerBound {
			// The bucket size calculation has a -1 because numRange does not
			// include values equal to upperBound.
			bucketSize := upperBound - lowerBound - 1
			numRange := int64(0)
			if bucketSize > 0 {
				var bucketMatchSize int64
				if op == geOp {
					// The matching values include val for geOp.
					bucketMatchSize = upperBound - val
				} else { /* op == gtOp */
					// The matching values do not include val for gtOp.
					bucketMatchSize = upperBound - val - 1
				}

				// Assuming a uniform distribution.
				numRange = (int64)(float64(b.numRange) * float64(bucketMatchSize) / float64(bucketSize))
			}
			buc := b
			buc.numRange = numRange
			newBuckets = append(newBuckets, buc)
			break
		}

		newBuckets = append(newBuckets, b)
	}

	// Reverse the buckets so they are sorted in ascending order.
	for i, j := 0, len(newBuckets)-1; i < j; i, j = i+1, j-1 {
		newBuckets[i], newBuckets[j] = newBuckets[j], newBuckets[i]
	}

	// Find the new lower bound of the histogram.
	var newLowerBound int64
	if op == geOp {
		newLowerBound = val - 1
	} else { /* op == gtOp */
		newLowerBound = val
	}
	v := &tree.NumVal{Value: constant.MakeInt64(newLowerBound)}
	lb, err := v.ResolveAsType(nil, types.Int)
	if err != nil {
		fatalf("could not create lower bound Datum: %s: %v", v, err)
	}
	return h.newHistogram(newBuckets, lb)
}
