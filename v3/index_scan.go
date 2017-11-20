package v3

import "bytes"

func init() {
	registerOperator(indexScanOp, "index scan", indexScan{})
}

var implicitPrimaryKey = &tableKey{name: "primary", primary: true}

func newIndexScanExpr(table *table, key *tableKey, scanProps *relationalProps) *expr {
	if key == nil {
		for i := range table.keys {
			k := &table.keys[i]
			if k.primary {
				key = k
				break
			}
		}
		if key == nil {
			key = implicitPrimaryKey
		}
	}

	index := *table
	index.name += "@" + key.name
	indexScan := &expr{
		op:      indexScanOp,
		private: &index,
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
	return relationalKind
}

func (indexScan) layout() exprLayout {
	return exprLayout{}
}

func (indexScan) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
}

func (indexScan) initKeys(e *expr, state *queryState) {
}

func (indexScan) updateProps(e *expr) {
	e.props.outerCols = e.requiredInputCols().Difference(e.props.outputCols)
}

func (indexScan) requiredProps(required *physicalProps, child int) *physicalProps {
	return nil
}
