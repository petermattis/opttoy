package v3

func init() {
	registerXform(scanToIndexScan{})
}

type scanToIndexScan struct {
	xformImplementation
}

func (scanToIndexScan) id() xformID {
	return xformScanToIndexScanID
}

func (scanToIndexScan) pattern() *expr {
	return &expr{
		op: scanOp,
	}
}

func (scanToIndexScan) check(e *expr) bool {
	return true
}

func (scanToIndexScan) apply(e *expr, results []*expr) []*expr {
	table := e.private.(*table)
	indexScan := newIndexScanExpr(table, table.getPrimaryKey(), e.props)
	results = append(results, indexScan)

	// For any index that can satisfy the output columns, generate an index scan
	// expression.
	for i := range table.keys {
		key := &table.keys[i]
		if key.primary {
			continue
		}
		indexScan := newIndexScanExpr(table, key, e.props)
		if !e.props.outputCols.SubsetOf(indexScan.props.outputCols) {
			continue
		}
		indexScan.props.outputCols = e.props.outputCols
		indexScan.loc = memoLoc{e.loc.group, -1}
		results = append(results, indexScan)
	}
	return results
}
