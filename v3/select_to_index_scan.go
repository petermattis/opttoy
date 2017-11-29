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
	for i := range table.keys {
		key := &table.keys[i]
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

			indexScan := newIndexScanExpr(table, key, scan.props)
			if !scan.props.outputCols.SubsetOf(indexScan.props.outputCols) {
				primaryScan := newIndexScanExpr(table, table.getPrimaryKey(), scan.props)
				var projections []*expr
				for _, col := range scan.props.columns {
					if indexScan.props.outputCols.Contains(col.index) {
						continue
					}
					projections = append(projections, col.newVariableExpr(""))
				}
				primaryScan.addProjections(projections)

				// TODO(peter): need to add a join condition on the columns of the
				// primary key.
				indexScan = &expr{
					op: innerJoinOp,
					children: []*expr{
						indexScan,   // left
						primaryScan, // right
						nil,         // filter
					},
					props: scan.props,
				}
				indexScan.initProps()
			}
			indexScan.loc = memoLoc{group: scan.loc.group, expr: -1}

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
