package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(indexScanOp, "index-scan", indexScan{})
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

type indexScan struct{}

func (indexScan) kind() operatorKind {
	return physicalKind | relationalKind
}

func (indexScan) layout() exprLayout {
	return exprLayout{
		numAux:       1,
		aggregations: -1,
		filters:      -1,
		groupings:    -1,
		projections:  0,
	}
}

func (indexScan) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "projections", e.projections(), level)
}

func (indexScan) initKeys(e *expr, state *queryState) {
}

func (indexScan) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
}

func (indexScan) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
