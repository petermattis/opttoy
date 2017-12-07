package v3

import (
	"fmt"
)

func init() {
	registerOperator(indexScanOp, "index-scan", indexScanClass{})
}

func newIndexScanExpr(table *table, key *tableKey, scanProps *relationalProps) *expr {
	index := *table
	index.name = tableName(fmt.Sprintf("%s@%s", table.name, key.name))
	indexScan := &expr{
		op:       indexScanOp,
		children: []*expr{nil /* projections */},
		private:  &index,
	}

	// Make index scans on the primary index retrieve all columns.
	if key.primary {
		indexScan.props = scanProps
	} else {
		indexScan.props = &relationalProps{
			columns: make([]columnProps, 0, len(key.columns)),
		}
		for _, i := range key.columns {
			indexScan.props.columns = append(indexScan.props.columns, scanProps.columns[i])
		}
		indexScan.initProps()

		// Add in any columns of the primary key that were not specified in the
		// index.
		primaryKey := table.getPrimaryKey()
		for _, i := range primaryKey.columns {
			if indexScan.props.outputCols.Contains(scanProps.columns[i].index) {
				continue
			}
			indexScan.props.columns = append(indexScan.props.columns, scanProps.columns[i])
		}
		indexScan.initProps()
	}

	indexScan.physicalProps = &physicalProps{
		providedOrdering: make(ordering, 0, len(key.columns)),
	}
	for _, i := range key.columns {
		indexScan.physicalProps.providedOrdering =
			append(indexScan.physicalProps.providedOrdering, scanProps.columns[i].index)
	}
	return indexScan
}

type indexScanClass struct{}

var _ operatorClass = indexScanClass{}

func (indexScanClass) kind() operatorKind {
	return physicalKind | relationalKind
}

func (indexScanClass) layout() exprLayout {
	return exprLayout{
		numAux:       1,
		aggregations: -1,
		filters:      -1,
		groupings:    -1,
		projections:  0,
	}
}

func (indexScanClass) format(e *expr, tp *treePrinter) {
	formatRelational(e, tp)
	formatExprs(tp, "projections", e.projections())
}

func (indexScanClass) initKeys(e *expr, state *queryState) {
}

func (indexScanClass) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
}

func (indexScanClass) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
