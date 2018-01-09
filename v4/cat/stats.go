package cat

import (
	"bytes"
	"fmt"
	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/gogo/protobuf/sortkeys"
)

// A histogram struct stores statistics for a table column, as well as
// buckets representing the distribution of non-NULL values.
//
// The statistics calculated on the base table will be 100% accurate
// at the time of collection (except for DistinctCount, which is an estimate).
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
type Histogram struct {
	// The total number of rows in the table.
	RowCount int64

	// The estimated cardinality (distinct values) for the column.
	DistinctCount int64

	// The number of NULL values for the column.
	NullCount int64

	// The histogram buckets which describe the distribution of non-NULL
	// values. The buckets are sorted by bucket.UpperBound.
	// The first bucket must have NumRange = 0, so the UpperBound
	// of the bucket indicates the lower bound of the histogram.
	Buckets []Bucket
}

func (h *Histogram) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "rows:       %d\n", h.RowCount)
	fmt.Fprintf(&buf, "distinct:   %d\n", h.DistinctCount)
	fmt.Fprintf(&buf, "nulls:      %d\n", h.NullCount)
	fmt.Fprintf(&buf, "buckets:   ")
	if len(h.Buckets) == 0 {
		fmt.Fprintf(&buf, " none")
	}
	for _, b := range h.Buckets {
		fmt.Fprintf(&buf, " %s:%d,%d", b.UpperBound, b.NumRange, b.NumEq)
	}
	buf.WriteString("\n")
	return buf.String()
}

// getLowerBound gets the exclusive lower bound on the
// histogram. i.e., it returns one less than the minimum value
// in the histogram.
//
// It panics if the histogram is empty or if NumRange is not
// zero in the first bucket.
func (h *Histogram) GetLowerBound() int64 {
	if len(h.Buckets) == 0 {
		panic("Called getLowerBound on empty histogram")
	}

	if h.Buckets[0].NumRange != 0 {
		panic("First bucket must have NumRange = 0")
	}

	return (int64)(*h.Buckets[0].UpperBound.(*tree.DInt)) - 1
}

// getUpperBound gets the inclusive upper bound on the
// histogram. i.e., it returns the maximum value
// in the histogram.
//
// It panics if the histogram is empty.
func (h *Histogram) GetUpperBound() int64 {
	if len(h.Buckets) == 0 {
		panic("Called getUpperBound on empty histogram")
	}

	return (int64)(*h.Buckets[len(h.Buckets)-1].UpperBound.(*tree.DInt))
}

func (h *Histogram) Validate() {
	checkBucketsValid(h.Buckets)
}

// FilterHistogramLtOpLeOp applies a filter to the histogram that compares
// the histogram column value to a constant value with a ltOp or leOp (e.g., x < 4).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *Histogram) FilterHistogramLtOpLeOp(op tree.ComparisonOperator, val int64) *Histogram {
	if op != tree.LT && op != tree.LE {
		panic("filterHistogramLtOpLeOp called with operator " + op.String())
	}

	if len(h.Buckets) == 0 {
		return h
	}

	lowerBound := h.GetLowerBound()
	var newBuckets []Bucket

	for _, b := range h.Buckets {
		if val <= lowerBound {
			break
		}

		upperBound := (int64)(*b.UpperBound.(*tree.DInt))
		if val <= upperBound {
			var buc Bucket
			if val < upperBound {
				buc, _ = b.splitBucket(val, lowerBound)
			} else {
				buc = b
			}

			if op == tree.LT {
				buc.NumEq = 0
			}
			newBuckets = append(newBuckets, buc)
			break
		}

		newBuckets = append(newBuckets, b)
		lowerBound = upperBound
	}

	return h.filterHistogram(newBuckets)
}

// FilterHistogramGtOpGeOp applies a filter to the histogram that compares
// the histogram column value to a constant value with a gtOp or geOp (e.g., x > 4).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *Histogram) FilterHistogramGtOpGeOp(op tree.ComparisonOperator, val int64) *Histogram {
	if op != tree.GT && op != tree.GE {
		panic("filterHistogramGtOpGeOp called with operator " + op.String())
	}

	if len(h.Buckets) == 0 {
		return h
	}

	upperBound := h.GetUpperBound()
	var newBuckets []Bucket

	newLowerBound := val
	if op == tree.GE {
		newLowerBound -= 1
	}

	// Iterate backwards through the buckets to avoid scanning buckets
	// that don't satisfy the predicate.
	for i := len(h.Buckets) - 1; i >= 0; i-- {
		b := h.Buckets[i]
		if val >= upperBound {
			if val == upperBound && op == tree.GE {
				buc := b
				buc.NumRange = 0
				newBuckets = append(newBuckets, buc)
			}
			break
		}

		var lowerBound int64
		if i == 0 {
			lowerBound = upperBound - 1
		} else {
			lowerBound = (int64)(*h.Buckets[i-1].UpperBound.(*tree.DInt))
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
	if len(newBuckets) > 0 && newBuckets[len(newBuckets)-1].NumRange != 0 {
		buc := Bucket{UpperBound: makeDatum(newLowerBound)}
		newBuckets = append(newBuckets, buc)
	}

	// Reverse the buckets so they are sorted in ascending order.
	for i, j := 0, len(newBuckets)-1; i < j; i, j = i+1, j-1 {
		newBuckets[i], newBuckets[j] = newBuckets[j], newBuckets[i]
	}

	return h.filterHistogram(newBuckets)
}

// FilterHistogramEqOpInOp applies a filter to the histogram that compares
// the histogram column value to a constant value or set of values with an
// eqOp (e.g., x == 4) or an inOp (e.g., x in (4, 5, 6)).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *Histogram) FilterHistogramEqOpInOp(vals []int64) *Histogram {
	if len(vals) == 0 {
		return &Histogram{}
	}

	if len(h.Buckets) == 0 {
		return h
	}

	sortkeys.Int64s(vals)
	valIdx := 0
	lowerBound := h.GetLowerBound()
	var newBuckets []Bucket

	for _, b := range h.Buckets {
		if valIdx >= len(vals) {
			break
		}

		for valIdx < len(vals) && vals[valIdx] <= lowerBound {
			valIdx++
		}

		upperBound := (int64)(*b.UpperBound.(*tree.DInt))
		bucketSize := upperBound - lowerBound - 1
		for valIdx < len(vals) && vals[valIdx] < upperBound && bucketSize > 0 {
			// Assuming a uniform distribution.
			numEq := (int64)(float64(b.NumRange) / float64(bucketSize))
			buc := Bucket{NumEq: numEq, UpperBound: makeDatum(vals[valIdx])}
			newBuckets = append(newBuckets, buc)
			valIdx++
		}

		for valIdx < len(vals) && vals[valIdx] == upperBound {
			buc := b
			buc.NumRange = 0
			newBuckets = append(newBuckets, buc)
			valIdx++
		}

		lowerBound = upperBound
	}

	return h.filterHistogram(newBuckets)
}

// FilterHistogramNeOpNotInOp applies a filter to the histogram that compares
// the histogram column value to a constant value or set of values with a
// neOp (e.g., x != 4) or notInOp (e.g., x not in (4, 5, 6)).
// Returns an updated histogram including only the values that satisfy the predicate.
//
// Currently only works for integer valued columns. This will need to be altered
// for floating point columns and other types.
func (h *Histogram) FilterHistogramNeOpNotInOp(vals []int64) *Histogram {
	if len(vals) == 0 || len(h.Buckets) == 0 {
		return h
	}

	sortkeys.Int64s(vals)
	valIdx := 0
	lowerBound := h.GetLowerBound()
	var newBuckets []Bucket

	for _, b := range h.Buckets {
		for valIdx < len(vals) && vals[valIdx] <= lowerBound {
			valIdx++
		}

		buc := b
		upperBound := (int64)(*b.UpperBound.(*tree.DInt))
		for valIdx < len(vals) && vals[valIdx] > lowerBound && vals[valIdx] < upperBound {
			var bucLower Bucket
			// Upper bucket will either be split again or added once this inner
			// loop terminates.
			bucLower, buc = buc.splitBucket(vals[valIdx], lowerBound)
			bucLower.NumEq = 0
			newBuckets = append(newBuckets, bucLower)
			lowerBound = vals[valIdx]
			valIdx++
		}

		for valIdx < len(vals) && vals[valIdx] == upperBound {
			buc.NumEq = 0
			valIdx++
		}

		newBuckets = append(newBuckets, buc)
		lowerBound = upperBound
	}

	return h.filterHistogram(newBuckets)
}

// NewHistogram creates a new histogram given new buckets which represent
// a filtered version of the existing histogram h.
func (h *Histogram) filterHistogram(newBuckets []Bucket) *Histogram {
	checkBucketsValid(newBuckets)

	total := int64(0)
	for _, b := range newBuckets {
		total += b.NumEq + b.NumRange
	}

	if total == 0 {
		return &Histogram{}
	}

	selectivity := float64(total) / float64(h.RowCount)

	// Estimate the new DistinctCount based on the selectivity of this filter.
	// todo(rytaft): this could be more precise if we take into account the
	// null count of the original histogram. This could also be more precise for
	// the operators =, !=, in, and not in, since we know how these operators
	// should affect the distinct count.
	distinctCount := int64(float64(h.DistinctCount) * selectivity)
	if distinctCount == 0 {
		// There must be at least one distinct value since RowCount > 0.
		distinctCount++
	}

	return &Histogram{
		RowCount:      total,
		DistinctCount: distinctCount,

		// All the returned rows will be non-null for this column.
		NullCount: 0,
		Buckets:   newBuckets,
	}
}

type Bucket struct {
	// The number of values in the bucket equal to UpperBound.
	NumEq int64

	// The number of values in the bucket, excluding those that are
	// equal to UpperBound.
	NumRange int64

	// The upper boundary of the bucket. The column values for the upper bound
	// are encoded using the ascending key encoding.
	UpperBound tree.Datum
}

// splitBucket splits a bucket into two buckets at the given split point.
// The lower bucket contains the values less than or equal to splitPoint, and the
// upper bucket contains the values greater than splitPoint. The count of values
// in NumRange is split between the two buckets assuming a uniform distribution.
//
// lowerBound  is an exclusive lower bound on the bucket (it's equal to one less
// than the minimum value).
func (b Bucket) splitBucket(splitPoint, lowerBound int64) (Bucket, Bucket) {
	upperBound := (int64)(*b.UpperBound.(*tree.DInt))

	// The bucket size calculation has a -1 because NumRange does not
	// include values equal to UpperBound.
	bucketSize := upperBound - lowerBound - 1
	if bucketSize <= 0 {
		panic("empty bucket should have been skipped")
	}

	if splitPoint >= upperBound || splitPoint <= lowerBound {
		panic(fmt.Sprintf("splitPoint (%d) must be between UpperBound (%d) and lowerBound (%d)",
			splitPoint, upperBound, lowerBound))
	}

	// Make the lower bucket.
	lowerMatchSize := splitPoint - lowerBound - 1
	lowerNumRange := (int64)(float64(b.NumRange) * float64(lowerMatchSize) / float64(bucketSize))
	lowerNumEq := (int64)(float64(b.NumRange) / float64(bucketSize))
	bucLower := Bucket{NumEq: lowerNumEq, NumRange: lowerNumRange, UpperBound: makeDatum(splitPoint)}

	// Make the upper bucket.
	upperMatchSize := upperBound - splitPoint - 1
	bucUpper := b
	bucUpper.NumRange = (int64)(float64(b.NumRange) * float64(upperMatchSize) / float64(bucketSize))

	return bucLower, bucUpper
}

// checkBucketsValid checks that the given buckets
// are valid histogram buckets, and panics if they are not valid.
func checkBucketsValid(buckets []Bucket) {
	if len(buckets) == 0 {
		return
	}

	if buckets[0].NumRange != 0 {
		panic("First bucket must have NumRange = 0")
	}

	prev := buckets[0].UpperBound
	for i := 1; i < len(buckets); i++ {
		cur := buckets[i].UpperBound
		if prev.Compare(nil /* ctx */, cur) >= 0 {
			panic("Buckets must be disjoint and ordered by UpperBound")
		}
		prev = cur
	}
}

func makeDatum(val int64) tree.Datum {
	v := &tree.NumVal{Value: constant.MakeInt64(val)}
	datum, err := v.ResolveAsType(nil, types.Int)
	if err != nil {
		fatalf("could not create Datum: %s: %v", v, err)
	}

	return datum
}
