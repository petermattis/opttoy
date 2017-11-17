package v3

func init() {
	registerXform(selectToIndexScan{})
}

type selectToIndexScan struct {
	xformImplementation
}

func (selectToIndexScan) id() xformID {
	return xformSelectToIndexScanID
}

func (selectToIndexScan) pattern() *expr {
	return &expr{
		op: selectOp,
		children: []*expr{
			&expr{ // left
				op: scanOp,
			},
			patternTree, // filter
		},
	}
}

func (selectToIndexScan) check(e *expr) bool {
	return true
}

func (selectToIndexScan) apply(e *expr, results []*expr) []*expr {
	// TODO(peter): Note that this logic is simplistic and incomplete. We really
	// want to be translating the filters into a set of per-column constraints.

	scan := e.children[0]
	table := scan.private.(*table)
	for _, key := range table.keys {
		// If the first column of the index is the variable used for a filter,
		// output an index scan expression.
		col0 := scan.props.columns[key.columns[0]]
		for _, filter := range e.filters() {
			// TODO(peter): this is ugly.
			switch filter.op {
			case eqOp:
			case ltOp:
			case gtOp:
			case leOp:
			case geOp:
				break
			default:
				continue
			}
			if filter.children[0].op != variableOp ||
				filter.children[1].op != constOp {
				continue
			}
			if !filter.children[0].scalarProps.inputCols.Contains(col0.index) {
				continue
			}

			index := *table
			index.name += "@" + key.name
			indexScan := &expr{
				op:      indexScanOp,
				private: &index,
			}
			// TODO(peter): hack to make index scans on the primary index retrieve
			// all columns.
			if key.name == "primary" {
				indexScan.props = scan.props
			} else {
				indexScan.props = &relationalProps{
					columns: make([]columnProps, 0, len(key.columns)),
				}
				for _, i := range key.columns {
					indexScan.props.columns = append(indexScan.props.columns, scan.props.columns[i])
				}
				indexScan.initProps()
			}

			if !scan.props.outputCols.SubsetOf(indexScan.props.outputCols) {
				indexScan = &expr{
					op: indexJoinOp,
					children: []*expr{
						indexScan,
					},
					props: scan.props,
				}
				indexScan.initProps()
			}

			results = append(results, &expr{
				op: selectOp,
				children: []*expr{
					indexScan,
					e.children[1],
				},
				props: e.props,
			})
		}
	}
	return results
}
