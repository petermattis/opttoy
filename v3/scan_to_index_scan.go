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
	index := *table
	index.name += "@primary"

	results = append(results, &expr{
		op:      indexScanOp,
		props:   e.props,
		private: &index,
	})

	// TODO(peter): for any index that can satisfy the output columns, generate
	// an index scan expression.
	return results
}
