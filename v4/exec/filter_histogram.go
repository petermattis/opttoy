package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/petermattis/opttoy/v4/cat"
)

type filterHistogram struct {
	catalog *cat.Catalog
}

// FilterHistogram filters a histogram based on the WHERE clause in the given select
// statement, and returns the histogram.  It expects a statement of the form:
//   SELECT * from histogram.table.column WHERE ...
// Currently the only operators supported in the WHERE clause are <, <=, >, and >=.
func (fh *filterHistogram) execute(
	tblName cat.TableName,
	colName cat.ColumnName,
	sel *tree.SelectClause,
) *cat.Histogram {
	// Get the histogram from the catalog.
	tbl := fh.catalog.Table(tblName)
	col := tbl.Column(colName)
	hist := col.Stats

	// Filter the histogram.
	expr, ok := sel.Where.Expr.(*tree.ComparisonExpr)
	if !ok {
		unimplemented("%s", sel)
	}

	var err error
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
				unimplemented("%s", sel)
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

	switch expr.Operator {
	case tree.LT, tree.LE:
		return hist.FilterHistogramLtOpLeOp(expr.Operator, val)
	case tree.GT, tree.GE:
		return hist.FilterHistogramGtOpGeOp(expr.Operator, val)
	case tree.EQ, tree.In:
		return hist.FilterHistogramEqOpInOp(vals)
	case tree.NE, tree.NotIn:
		return hist.FilterHistogramNeOpNotInOp(vals)
	default:
		unimplemented("%s", sel)
	}

	return nil
}
