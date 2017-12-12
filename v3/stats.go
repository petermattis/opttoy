package v3

import (
	"bytes"
	"fmt"
	"sort"

	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/gogo/protobuf/sortkeys"
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

// A histogram struct stores statistics for a table column, as well as
// buckets representing the distribution of non-NULL values.
//
// The statistics calculated on the base table will be 100% accurate
// at the time of collection (except for distinctCount, which is an estimate).
// Statistics become stale quickly, however, if the table is updated
// frequently.  This struct does not currently include any estimate of
// the error due to staleness.
//
// For histograms representing intermediate states in the query tree,
// there is an additional source of error due to lack of information
// about the distribution of values within each histogram bucket at
// the base of the query tree. For example, when a bucket is split,
// we calculate the size of the new buckets by assuming that values are
// uniformly distributed across the original bucket.  The histogram struct does
// not currently include any estimate of the error due to data distribution
// within buckets.
type histogram struct {
	// The total number of rows in the table.
	rowCount int64
	// The estimated cardinality (distinct values) for the column.
	distinctCount int64
	// The number of NULL values for the column.
	nullCount int64
	// The histogram buckets which describe the distribution of non-NULL
	// values. The buckets are sorted by bucket.upperBound.
	// The first bucket must have numRange = 0, so the upperBound
	// of the bucket indicates the lower bound of the histogram.
	buckets []bucket
}

func (h *histogram) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "rows:       %d\n", h.rowCount)
	fmt.Fprintf(&buf, "distinct:   %d\n", h.distinctCount)
	fmt.Fprintf(&buf, "nulls:      %d\n", h.nullCount)
	fmt.Fprintf(&buf, "buckets:   ")
	if len(h.buckets) == 0 {
		fmt.Fprintf(&buf, " none")
	}
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
	var val int64
	var vals []int64
	switch v := expr.Right.(type) {
	case *tree.NumVal:
		val, err = v.AsInt64()
		if err != nil {
			fatalf("unable to cast datum to int64: %v", err)
		}
		vals = []int64{val}
	case *tree.Tuple:
		for _, elem := range v.Exprs {
			numVal, ok := elem.(*tree.NumVal)
			if !ok {
				unimplemented("%s", stmt)
			}
			val, err = numVal.AsInt64()
			if err != nil {
				fatalf("unable to cast datum to int64: %v", err)
			}
			vals = append(vals, val)
		}
	default:
		unimplemented("%T", v)
	}

	switch op {
	case ltOp, leOp:
		return hist.filterHistogramLtOpLeOp(op, val)
	case gtOp, geOp:
		return hist.filterHistogramGtOpGeOp(op, val)
	case eqOp, inOp:
		return hist.filterHistogramEqOpInOp(vals)
	case neOp, notInOp:
		return hist.filterHistogramNeOpNotInOp(vals)
	default:
		unimplemented("%s", stmt)
	}

	return nil
}

func makeDatum(val int64) tree.Datum {
	v := &tree.NumVal{Value: constant.MakeInt64(val)}
	datum, err := v.ResolveAsType(nil, types.Int)
	if err != nil {
		fatalf("could not create Datum: %s: %v", v, err)
	}

	return datum
}

func newBucket(upperBound, numRange, numEq int64) bucket {
	return bucket{
		numEq:      numEq,
		numRange:   numRange,
		upperBound: makeDatum(upperBound),
	}
}

// splitBucket splits a bucket into two buckets at the given split point.
// The lower bucket contains the values less than or equal to splitPoint, and the
// upper bucket contains the values greater than splitPoint. The count of values
// in numRange is split between the two buckets assuming a uniform distribution.
//
// lowerBound  is an exclusive lower bound on the bucket (it's equal to one less
// than the minimum value).
func (b bucket) splitBucket(splitPoint, lowerBound int64) (bucket, bucket) {
	upperBound := (int64)(*b.upperBound.(*tree.DInt))

	// The bucket size calculation has a -1 because numRange does not
	// include values equal to upperBound.
	bucketSize := upperBound - lowerBound - 1
	if bucketSize <= 0 {
		panic("empty bucket should have been skipped")
	}

	if splitPoint >= upperBound || splitPoint <= lowerBound {
		panic(fmt.Sprintf("splitPoint (%d) must be between upperBound (%d) and lowerBound (%d)",
			splitPoint, upperBound, lowerBound))
	}

	// Make the lower bucket.
	lowerMatchSize := splitPoint - lowerBound - 1
	lowerNumRange := (int64)(float64(b.numRange) * float64(lowerMatchSize) / float64(bucketSize))
	lowerNumEq := (int64)(float64(b.numRange) / float64(bucketSize))
	bucLower := newBucket(splitPoint, lowerNumRange, lowerNumEq)

	// Make the upper bucket.
	upperMatchSize := upperBound - splitPoint - 1
	bucUpper := b
	bucUpper.numRange = (int64)(float64(b.numRange) * float64(upperMatchSize) / float64(bucketSize))

	return bucLower, bucUpper
}

// newHistogram creates a new histogram given new buckets and a new lower bound,
// which represent a filtered version of the existing histogram h.
func (h *histogram) newHistogram(newBuckets []bucket) *histogram {
	total := int64(0)
	for _, b := range newBuckets {
		total += b.numEq + b.numRange
	}

	if total == 0 {
		return &histogram{}
	}

	selectivity := float64(total) / float64(h.rowCount)

	// Estimate the new distinctCount based on the selectivity of this filter.
	// todo(rytaft): this could be more precise if we take into account the
	// null count of the original histogram. This could also be more precise for
	// the operators =, !=, in, and not in, since we know how these operators
	// should affect the distinct count.
	distinctCount := int64(float64(h.distinctCount) * selectivity)
	if distinctCount == 0 {
		// There must be at least one distinct value since rowCount > 0.
		distinctCount++
	}
	return &histogram{
		rowCount:      total,
		distinctCount: distinctCount,

		// All the returned rows will be non-null for this column.
		nullCount: 0,
		buckets:   newBuckets,
	}
}

// filterHistogramLtOpLeOp applies a filter to the histogram that compares
// the histogram column value to a constant value with a ltOp or leOp (e.g., x < 4).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *histogram) filterHistogramLtOpLeOp(op operator, val int64) *histogram {
	if op != ltOp && op != leOp {
		panic("filterHistogramLtOpLeOp called with operator " + op.String())
	}

	if len(h.buckets) == 0 {
		return h
	}

	lowerBound := (int64)(*h.buckets[0].upperBound.(*tree.DInt)) - 1
	var newBuckets []bucket

	for _, b := range h.buckets {
		if val <= lowerBound {
			break
		}

		upperBound := (int64)(*b.upperBound.(*tree.DInt))
		if val <= upperBound {
			var buc bucket
			if val < upperBound {
				buc, _ = b.splitBucket(val, lowerBound)
			} else {
				buc = b
			}

			if op == ltOp {
				buc.numEq = 0
			}
			newBuckets = append(newBuckets, buc)
			break
		}

		newBuckets = append(newBuckets, b)
		lowerBound = upperBound
	}

	return h.newHistogram(newBuckets)
}

// filterHistogramGtOpGeOp applies a filter to the histogram that compares
// the histogram column value to a constant value with a gtOp or geOp (e.g., x > 4).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *histogram) filterHistogramGtOpGeOp(op operator, val int64) *histogram {
	if op != gtOp && op != geOp {
		panic("filterHistogramGtOpGeOp called with operator " + op.String())
	}

	if len(h.buckets) == 0 {
		return h
	}

	upperBound := (int64)(*h.buckets[len(h.buckets)-1].upperBound.(*tree.DInt))
	var newBuckets []bucket

	newLowerBound := val
	if op == geOp {
		newLowerBound -= 1
	}

	for i := len(h.buckets) - 1; i >= 0; i-- {
		b := h.buckets[i]
		if val >= upperBound {
			if val == upperBound && op == geOp {
				buc := b
				buc.numRange = 0
				newBuckets = append(newBuckets, buc)
			}
			break
		}

		var lowerBound int64
		if i == 0 {
			lowerBound = upperBound - 1
		} else {
			lowerBound = (int64)(*h.buckets[i-1].upperBound.(*tree.DInt))
		}
		if val > lowerBound {
			_, buc := b.splitBucket(newLowerBound, lowerBound)
			newBuckets = append(newBuckets, buc)
			break
		}

		newBuckets = append(newBuckets, b)
		upperBound = lowerBound
	}

	// Add a dummy bucket for the lower bound if needed.
	if len(newBuckets) > 0 && newBuckets[len(newBuckets)-1].numRange != 0 {
		buc := newBucket(newLowerBound, 0 /* numRange */, 0 /* numEq */)
		newBuckets = append(newBuckets, buc)
	}

	// Reverse the buckets so they are sorted in ascending order.
	for i, j := 0, len(newBuckets)-1; i < j; i, j = i+1, j-1 {
		newBuckets[i], newBuckets[j] = newBuckets[j], newBuckets[i]
	}

	return h.newHistogram(newBuckets)
}

// filterHistogramEqOpInOp applies a filter to the histogram that compares
// the histogram column value to a constant value or set of values with an
// eqOp (e.g., x == 4) or an inOp (e.g., x in (4, 5, 6)).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *histogram) filterHistogramEqOpInOp(vals []int64) *histogram {
	if len(vals) == 0 {
		return &histogram{}
	}

	if len(h.buckets) == 0 {
		return h
	}

	sortkeys.Int64s(vals)
	valIdx := 0
	lowerBound := (int64)(*h.buckets[0].upperBound.(*tree.DInt)) - 1
	var newBuckets []bucket

	for _, b := range h.buckets {
		if valIdx >= len(vals) {
			break
		}

		for valIdx < len(vals) && vals[valIdx] <= lowerBound {
			valIdx++
		}

		upperBound := (int64)(*b.upperBound.(*tree.DInt))
		bucketSize := upperBound - lowerBound - 1
		for valIdx < len(vals) && vals[valIdx] < upperBound && bucketSize > 0 {
			// Assuming a uniform distribution.
			numEq := (int64)(float64(b.numRange) / float64(bucketSize))
			buc := newBucket(vals[valIdx], 0 /* numRange */, numEq)
			newBuckets = append(newBuckets, buc)
			valIdx++
		}

		for valIdx < len(vals) && vals[valIdx] == upperBound {
			buc := b
			buc.numRange = 0
			newBuckets = append(newBuckets, buc)
			valIdx++
		}

		lowerBound = upperBound
	}

	return h.newHistogram(newBuckets)
}

// filterHistogramNeOpNotInOp applies a filter to the histogram that compares
// the histogram column value to a constant value or set of values with a
// neOp (e.g., x != 4) or notInOp (e.g., x not in (4, 5, 6)).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *histogram) filterHistogramNeOpNotInOp(vals []int64) *histogram {
	if len(vals) == 0 || len(h.buckets) == 0 {
		return h
	}

	sortkeys.Int64s(vals)
	valIdx := 0
	lowerBound := (int64)(*h.buckets[0].upperBound.(*tree.DInt)) - 1
	var newBuckets []bucket

	for _, b := range h.buckets {
		for valIdx < len(vals) && vals[valIdx] <= lowerBound {
			valIdx++
		}

		buc := b
		upperBound := (int64)(*b.upperBound.(*tree.DInt))
		for valIdx < len(vals) && vals[valIdx] > lowerBound && vals[valIdx] < upperBound {
			var bucLower bucket
			// Upper bucket will either be split again or added once this inner
			// loop terminates.
			bucLower, buc = buc.splitBucket(vals[valIdx], lowerBound)
			bucLower.numEq = 0
			newBuckets = append(newBuckets, bucLower)
			lowerBound = vals[valIdx]
			valIdx++
		}

		for valIdx < len(vals) && vals[valIdx] == upperBound {
			buc.numEq = 0
			valIdx++
		}

		newBuckets = append(newBuckets, buc)
		lowerBound = upperBound
	}

	return h.newHistogram(newBuckets)
}
