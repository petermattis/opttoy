package v3

import (
	"go/constant"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"bytes"
)

type testBucket struct {
	numEq      int64
	numRange   int64
	upperBound int64
}

type testHistogram struct {
	rowCount      int64
	distinctCount int64
	nullCount     int64
	lowerBound    int64
	buckets       []testBucket
}

func histogramFromTestHistogram(t *testing.T, histograms []testHistogram) []histogram {
	out := make([]histogram, len(histograms))
	for i, h := range histograms {
		buckets := make([]bucket, len(h.buckets))
		for j, b := range h.buckets {
			v := &tree.NumVal{Value: constant.MakeInt64(b.upperBound)}
			ub, err := v.ResolveAsType(nil, types.Int)
			if err != nil {
				t.Fatalf("could not create upper bound Datum: %s: %v", v, err)
			}

			buckets[j] = bucket{
				numEq:      b.numEq,
				numRange:   b.numRange,
				upperBound: ub,
			}
		}

		v := &tree.NumVal{Value: constant.MakeInt64(h.lowerBound)}
		lb, err := v.ResolveAsType(nil, types.Int)
		if err != nil {
			t.Fatalf("could not create lower bound Datum: %s: %v", v, err)
		}

		out[i] = histogram{
			rowCount:      h.rowCount,
			distinctCount: h.distinctCount,
			nullCount:     h.nullCount,
			lowerBound:    lb,
			buckets:       buckets,
		}
	}

	return out
}

func TestHistogram(t *testing.T) {
	testHistograms := histogramFromTestHistogram(t, []testHistogram{
		{
			rowCount:      1000,
			distinctCount: 100,
			nullCount:     10,
			lowerBound:    0,
			buckets: []testBucket{
				{
					numEq:      3,
					numRange:   3,
					upperBound: 2,
				},
				{
					numEq:      2,
					numRange:   5,
					upperBound: 4,
				},
				{
					numEq:      1,
					numRange:   976,
					upperBound: 100,
				},
			},
		},
	})

	expectedHistograms := histogramFromTestHistogram(t, []testHistogram{
		{
			rowCount:      11,
			distinctCount: 1,
			nullCount:     0,
			lowerBound:    0,
			buckets: []testBucket{
				{
					numEq:      3,
					numRange:   3,
					upperBound: 2,
				},
				{
					numEq:      0,
					numRange:   5,
					upperBound: 4,
				},
			},
		},
		{
			rowCount:      13,
			distinctCount: 1,
			nullCount:     0,
			lowerBound:    0,
			buckets: []testBucket{
				{
					numEq:      3,
					numRange:   3,
					upperBound: 2,
				},
				{
					numEq:      2,
					numRange:   5,
					upperBound: 4,
				},
			},
		},
		{
			rowCount:      977,
			distinctCount: 97,
			nullCount:     0,
			lowerBound:    4,
			buckets: []testBucket{
				{
					numEq:      1,
					numRange:   976,
					upperBound: 100,
				},
			},
		},
		{
			rowCount:      979,
			distinctCount: 97,
			nullCount:     0,
			lowerBound:    3,
			buckets: []testBucket{
				{
					numEq:      2,
					numRange:   0,
					upperBound: 4,
				},
				{
					numEq:      1,
					numRange:   976,
					upperBound: 100,
				},
			},
		},
		{
			rowCount:      475,
			distinctCount: 47,
			nullCount:     0,
			lowerBound:    0,
			buckets: []testBucket{
				{
					numEq:      3,
					numRange:   3,
					upperBound: 2,
				},
				{
					numEq:      2,
					numRange:   5,
					upperBound: 4,
				},
				{
					numEq:      0,
					numRange:   462,
					upperBound: 50,
				},
			},
		},
		{
			rowCount:      485,
			distinctCount: 48,
			nullCount:     0,
			lowerBound:    0,
			buckets: []testBucket{
				{
					numEq:      3,
					numRange:   3,
					upperBound: 2,
				},
				{
					numEq:      2,
					numRange:   5,
					upperBound: 4,
				},
				{
					numEq:      0,
					numRange:   472,
					upperBound: 51,
				},
			},
		},
		{
			rowCount:      504,
			distinctCount: 50,
			nullCount:     0,
			lowerBound:    50,
			buckets: []testBucket{
				{
					numEq:      1,
					numRange:   503,
					upperBound: 100,
				},
			},
		},
		{
			rowCount:      514,
			distinctCount: 51,
			nullCount:     0,
			lowerBound:    49,
			buckets: []testBucket{
				{
					numEq:      1,
					numRange:   513,
					upperBound: 100,
				},
			},
		},
	})

	outputHistograms := make([]histogram, len(expectedHistograms))
	j := 0
	for i := range testHistograms {
		outputHistograms[j] = *testHistograms[i].filterHistogramLtOpLeOp(ltOp, 4)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramLtOpLeOp(leOp, 4)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramGtOpGeOp(gtOp, 4)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramGtOpGeOp(geOp, 4)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramLtOpLeOp(ltOp, 50)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramLtOpLeOp(leOp, 50)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramGtOpGeOp(gtOp, 50)
		j++
		outputHistograms[j] = *testHistograms[i].filterHistogramGtOpGeOp(geOp, 50)
		j++
	}

	if !reflect.DeepEqual(outputHistograms, expectedHistograms) {
		var buffer bytes.Buffer
		for i := range outputHistograms {
			if i != 0 {
				buffer.WriteString(",\n")
			}
			buffer.WriteString("{\n" + outputHistograms[i].String() + "}")
		}
		t.Fatalf("Output histograms do not match expected. Output: %s", buffer.String())
	}
}
