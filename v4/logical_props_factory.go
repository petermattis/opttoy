package v4

type logicalPropsFactory struct {
	memo *memo
	md   *metadata
}

func (f *logicalPropsFactory) init(memo *memo, md *metadata) {
	f.memo = memo
	f.md = md
}

func (f *logicalPropsFactory) constructProps(e *expr) logicalPropsID {
	if e.isScalar() {
		var props logicalProps
		f.setInputCols(&props.scalar, e)
		return f.memo.internLogicalProps(&props)
	}

	switch e.op {
	case scanOp:
		return f.constructScanProps(e)

	case leftJoinOp:
		return f.constructJoinProps(e)
	case rightJoinOp:
		return f.constructJoinProps(e)
	case fullJoinOp:
		return f.constructJoinProps(e)
	case semiJoinOp:
		return f.constructJoinProps(e)
	case antiJoinOp:
		return f.constructJoinProps(e)
	case innerJoinApplyOp:
		return f.constructJoinProps(e)
	case leftJoinApplyOp:
		return f.constructJoinProps(e)
	case rightJoinApplyOp:
		return f.constructJoinProps(e)
	case fullJoinApplyOp:
		return f.constructJoinProps(e)
	case semiJoinApplyOp:
		return f.constructJoinProps(e)
	case antiJoinApplyOp:
		return f.constructJoinProps(e)
	}

	fatalf("unrecognized expression type: %v", e.op)
	return 0
}

func (f *logicalPropsFactory) constructJoinProps(e *expr) logicalPropsID {
	var props relationalProps

	leftProps := &f.lookupLogicalProps(e.childGroup(0)).relational
	rightProps := &f.lookupLogicalProps(e.childGroup(1)).relational
	filterProps := &f.lookupLogicalProps(e.childGroup(2)).scalar

	// Output columns are concatenation of columns from left and right inputs.
	props.columns = make([]columnProps, len(leftProps.columns)+len(rightProps.columns))
	copy(props.columns[:], leftProps.columns)
	copy(props.columns[len(leftProps.columns):], rightProps.columns)
	f.setOutputCols(&props)

	// If column is not null in either left or right side of join, then result
	// is not null.
	props.notNullCols.UnionWith(leftProps.notNullCols)
	props.notNullCols.UnionWith(rightProps.notNullCols)

	// Any columns which are used by any of the join children, but are not
	// part of the join output columns are outer columns.
	props.outerCols = filterProps.inputCols.Difference(props.outputCols)
	props.outerCols.UnionWith(leftProps.outerCols)
	props.outerCols.UnionWith(rightProps.outerCols)

	// Union overlapping equivalent columns from inputs.
	props.addEquivColumnSets(leftProps.equivCols)
	props.addEquivColumnSets(rightProps.equivCols)

	return f.internRelationalProps(&props)
}

func (f *logicalPropsFactory) constructScanProps(e *expr) logicalPropsID {
	var props relationalProps

	tab := e.private().(*table)
	base := f.md.tables[tab.name]

	// Output columns are derived from table schema.
	props.columns = make([]columnProps, 0, len(tab.columns))
	for i, col := range tab.columns {
		index := base + colsetIndex(i)
		props.columns = append(props.columns, columnProps{
			index: index,
			name:  col.name,
			table: tab.name,
		})
	}

	f.setOutputCols(&props)

	// Initialize keys from the table schema.
	for _, k := range tab.keys {
		if k.fkey == nil && (k.primary || k.unique) {
			var key colset
			for _, i := range k.columns {
				key.Add(props.columns[i].index)
			}

			props.weakKeys = append(props.weakKeys, key)
		}
	}

	// Initialize not-NULL columns from the table schema.
	for i, col := range tab.columns {
		if col.notNull {
			props.notNullCols.Add(props.columns[i].index)
		}
	}

	return f.internRelationalProps(&props)
}

func (f *logicalPropsFactory) setOutputCols(props *relationalProps) {
	for _, col := range props.columns {
		props.outputCols.Add(col.index)
	}
}

func (f *logicalPropsFactory) setInputCols(props *scalarProps, e *expr) {
	for i := 0; i < e.childCount(); i++ {
		mgrp := e.memo.lookupGroup(e.childGroup(i))
		props.inputCols.UnionWith(e.memo.lookupLogicalProps(mgrp.logical).scalar.inputCols)
	}

	return
}

// Add additional not-NULL columns based on the filtering expression.
func (f *logicalPropsFactory) setPropsFromFilter(props *relationalProps, filter *expr) {
	// Expand the set of non-NULL columns based on the filter.
	//
	// TODO(peter): Need to make sure the filter is not null-tolerant.
	props.notNullCols.UnionWith(filter.logical().scalar.inputCols)

	f.setEquivProperties(props, filter)
}

func (f *logicalPropsFactory) setEquivProperties(props *relationalProps, filter *expr) {
	// Find equivalent columns.
	switch filter.operator() {
	case eqOp:
		left := filter.child(0)
		right := filter.child(1)

		if left.op == variableOp && right.op == variableOp {
			cols := left.logical().scalar.inputCols
			cols.UnionWith(right.logical().scalar.inputCols)
			props.addEquivColumns(cols)
		}

	case andOp:
		f.setEquivProperties(props, &filter.child(0))
		f.setEquivProperties(props, &filter.child(1))
	}

	// TODO(peter): Support tuple comparisons such as "(a, b) = (c, d)".
}

func (f *logicalPropsFactory) lookupLogicalProps(group groupID) *logicalProps {
	return f.memo.lookupLogicalProps(f.memo.lookupGroup(group).logical)
}

func (f *logicalPropsFactory) internRelationalProps(props *relationalProps) logicalPropsID {
	return f.memo.internLogicalProps(&logicalProps{relational: *props})
}
