package opt

type logicalPropsFactory struct {
	mem *memo
}

func (f *logicalPropsFactory) init(mem *memo) {
	f.mem = mem
}

func (f *logicalPropsFactory) constructProps(e *Expr) *LogicalProps {
	if e.IsRelational() {
		return f.constructRelationalProps(e)
	}

	return f.constructScalarProps(e)
}

func (f *logicalPropsFactory) constructRelationalProps(e *Expr) *LogicalProps {
	switch e.Operator() {
	case ScanOp:
		return f.constructScanProps(e)

	case ValuesOp:
		return &LogicalProps{}

	case SelectOp:
		return f.constructSelectProps(e)

	case ProjectOp:
		return f.constructProjectProps(e)

	case InnerJoinOp, LeftJoinOp, RightJoinOp, FullJoinOp,
		SemiJoinOp, AntiJoinOp, InnerJoinApplyOp, LeftJoinApplyOp,
		RightJoinApplyOp, FullJoinApplyOp, SemiJoinApplyOp, AntiJoinApplyOp:
		return f.constructJoinProps(e)

	case UnionOp:
		return f.constructSetProps(e)

	case GroupByOp:
		return f.constructGroupByProps(e)
	}

	fatalf("unrecognized relational expression type: %v", e.op)
	return nil
}

func (f *logicalPropsFactory) constructScanProps(e *Expr) *LogicalProps {
	var props LogicalProps

	tblIndex := e.Private().(TableIndex)
	tbl := f.mem.metadata.Table(tblIndex).Table

	// A table's output column indexes are contiguous.
	props.Relational.OutputCols.AddRange(int(tblIndex), int(tblIndex)+len(tbl.Columns)-1)

	// Initialize keys from the table schema.
	for _, k := range tbl.Keys {
		if k.Fkey == nil && (k.Primary || k.Unique) {
			var key ColSet
			for _, i := range k.Columns {
				key.Add(int(tblIndex) + int(i))
			}

			props.Relational.WeakKeys = append(props.Relational.WeakKeys, key)
		}
	}

	// Initialize not-NULL columns from the table schema.
	for i, col := range tbl.Columns {
		if col.NotNull {
			props.Relational.NotNullCols.Add(int(tblIndex) + i)
		}
	}

	return &props
}

func (f *logicalPropsFactory) constructSelectProps(e *Expr) *LogicalProps {
	var props LogicalProps

	inputProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	filterProps := f.mem.lookupGroup(e.ChildGroup(1)).logical

	// Inherit output columns from input.
	props.Relational.OutputCols = inputProps.Relational.OutputCols

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols

	// Any columns which are used by the input, but are not part of the select
	// output columns are unbound columns.
	props.UnboundCols = filterProps.UnboundCols.Difference(props.Relational.OutputCols)
	props.UnboundCols.UnionWith(inputProps.UnboundCols)

	// Inherit equivalent columns from input.
	props.Relational.EquivCols = inputProps.Relational.EquivCols

	// Set additional properties according to the join filter.
	filter := e.Child(1)
	f.addPropsFromFilter(&props, &filter, true)

	return &props
}

func (f *logicalPropsFactory) constructProjectProps(e *Expr) *LogicalProps {
	var props LogicalProps

	inputProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	projectionProps := f.mem.lookupGroup(e.ChildGroup(1)).logical

	// Use output columns from projection list.
	projections := e.Child(1)
	props.Relational.OutputCols = *projections.Private().(*ColSet)

	// Inherit not null columns from input. This may contain non-output
	// columns.
	// TODO(andy): is it OK to not intersect the set with output cols?
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols

	// Any columns which are used by any of the project children, but are not
	// part of the input columns are unbound columns.
	props.UnboundCols = projectionProps.UnboundCols.Difference(inputProps.Relational.OutputCols)
	props.UnboundCols.UnionWith(inputProps.UnboundCols)

	// Inherit equivalent columns from input. This may contain non-output
	// columns.
	props.Relational.EquivCols = inputProps.Relational.EquivCols

	return &props
}

func (f *logicalPropsFactory) constructJoinProps(e *Expr) *LogicalProps {
	var props LogicalProps

	leftProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	rightProps := f.mem.lookupGroup(e.ChildGroup(1)).logical
	filterProps := f.mem.lookupGroup(e.ChildGroup(2)).logical

	// Output columns are union of columns from left and right inputs, except
	// in case of semi-joins, which only project the left columns.
	props.Relational.OutputCols.UnionWith(leftProps.Relational.OutputCols)
	switch e.Operator() {
	case SemiJoinOp, AntiJoinOp, SemiJoinApplyOp, AntiJoinApplyOp:

	default:
		props.Relational.OutputCols.UnionWith(rightProps.Relational.OutputCols)
	}

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch e.Operator() {
	case LeftJoinOp, FullJoinOp, LeftJoinApplyOp, FullJoinApplyOp:

	default:
		props.Relational.NotNullCols.UnionWith(rightProps.Relational.NotNullCols)
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch e.Operator() {
	case RightJoinOp, FullJoinOp, RightJoinApplyOp, FullJoinApplyOp:

	default:
		props.Relational.NotNullCols.UnionWith(leftProps.Relational.NotNullCols)
	}

	// Any columns which are used by any of the join children, but are not
	// part of the join output columns are unbound columns.
	props.UnboundCols = filterProps.UnboundCols.Copy()
	props.UnboundCols.UnionWith(leftProps.UnboundCols)
	props.UnboundCols.UnionWith(rightProps.UnboundCols)
	props.UnboundCols.DifferenceWith(leftProps.Relational.OutputCols.Union(rightProps.Relational.OutputCols))

	// Union equivalent columns from inputs (these never overlap).
	props.Relational.EquivCols = append(props.Relational.EquivCols, leftProps.Relational.EquivCols...)
	switch e.Operator() {
	case SemiJoinOp, AntiJoinOp, SemiJoinApplyOp, AntiJoinApplyOp:

	default:
		props.Relational.EquivCols = append(props.Relational.EquivCols, rightProps.Relational.EquivCols...)
	}

	// Set additional properties according to the join filter.
	filter := e.Child(2)
	f.addPropsFromFilter(&props, &filter, false)

	return &props
}

func (f *logicalPropsFactory) constructGroupByProps(e *Expr) *LogicalProps {
	var props LogicalProps

	inputProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	groupingsProps := f.mem.lookupGroup(e.ChildGroup(1)).logical
	aggProps := f.mem.lookupGroup(e.ChildGroup(2)).logical

	// Output columns are union of columns from grouping and aggregate
	// projection lists.
	groupings := e.Child(1)
	props.Relational.OutputCols = groupings.Private().(*ColSet).Copy()
	agg := e.Child(2)
	props.Relational.OutputCols.UnionWith(*agg.Private().(*ColSet))

	// Find all unbound columns from the groupings or aggregation expressions
	// that are not bound by the input columns, and union those with unbound
	// columns from the input.
	props.UnboundCols = groupingsProps.UnboundCols.Copy()
	props.UnboundCols.UnionWith(aggProps.UnboundCols)
	props.UnboundCols.DifferenceWith(inputProps.Relational.OutputCols)
	props.UnboundCols.UnionWith(inputProps.UnboundCols)

	return &props
}

func (f *logicalPropsFactory) constructSetProps(e *Expr) *LogicalProps {
	var props LogicalProps

	leftProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	rightProps := f.mem.lookupGroup(e.ChildGroup(1)).logical
	colMap := *e.Private().(*ColMap)

	// Use left input's output columns.
	props.Relational.OutputCols = leftProps.Relational.OutputCols

	// Columns have to be not-null on both sides to be not-null in result.
	for leftIndex, rightIndex := range colMap {
		if !leftProps.Relational.NotNullCols.Contains(int(leftIndex)) {
			continue
		}
		if !rightProps.Relational.NotNullCols.Contains(int(rightIndex)) {
			continue
		}
		props.Relational.NotNullCols.Add(int(leftIndex))
	}

	// Unbound columns from either side are unbound in result.
	props.UnboundCols = leftProps.UnboundCols.Union(rightProps.UnboundCols)

	return &props
}

func (f *logicalPropsFactory) constructValuesProps(e *Expr) *LogicalProps {
	var props LogicalProps

	rowsProps := f.mem.lookupGroup(e.ChildGroup(0)).logical

	// Use output columns that are attached to the values op.
	props.Relational.OutputCols = *e.Private().(*ColSet)

	// Inherit unbound columns from rows expression.
	props.UnboundCols = rowsProps.UnboundCols

	return &props
}

// Add additional not-NULL columns based on the filtering expression.
func (f *logicalPropsFactory) addPropsFromFilter(props *LogicalProps, filter *Expr, copyOnWrite bool) {
	// Expand the set of non-NULL columns based on the filter.
	//
	// TODO(peter): Need to make sure the filter is not null-tolerant.
	if copyOnWrite {
		props.Relational.NotNullCols = props.Relational.NotNullCols.Copy()
	}
	props.Relational.NotNullCols.UnionWith(filter.Logical().UnboundCols)

	f.addEquivProperties(props, filter, copyOnWrite)
}

func (f *logicalPropsFactory) addEquivProperties(props *LogicalProps, filter *Expr, copyOnWrite bool) bool {
	// Find equivalent columns.
	switch filter.Operator() {
	case EqOp:
		left := filter.Child(0)
		right := filter.Child(1)

		if left.op == VariableOp && right.op == VariableOp {
			cols := left.Logical().UnboundCols.Union(right.Logical().UnboundCols)
			if copyOnWrite {
				props.Relational.EquivCols = props.Relational.EquivCols.Copy()
				copyOnWrite = false
			}
			props.addEquivColumns(cols)
		}

	case FiltersOp:
		for i := 0; i < filter.ChildCount(); i++ {
			child := filter.Child(i)
			copyOnWrite = f.addEquivProperties(props, &child, copyOnWrite)
		}
	}

	// TODO(peter): Support tuple comparisons such as "(a, b) = (c, d)".

	return copyOnWrite
}

func (f *logicalPropsFactory) constructScalarProps(e *Expr) *LogicalProps {
	switch e.Operator() {
	case VariableOp:
		return f.constructVariableProps(e)

	case SubqueryOp:
		return f.constructSubqueryProps(e)
	}

	// Union unbound props from all children by default.
	var props LogicalProps
	for i := 0; i < e.ChildCount(); i++ {
		props.UnboundCols.UnionWith(f.mem.lookupGroup(e.ChildGroup(i)).logical.UnboundCols)
	}
	return &props
}

func (f *logicalPropsFactory) constructVariableProps(e *Expr) *LogicalProps {
	var props LogicalProps
	props.UnboundCols.Add(int(e.Private().(ColumnIndex)))
	return &props
}

func (f *logicalPropsFactory) constructSubqueryProps(e *Expr) *LogicalProps {
	var props LogicalProps

	inputProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	projectionProps := f.mem.lookupGroup(e.ChildGroup(1)).logical

	// Unbound columns include those from the input and those from the
	// projection that are not satisfied by the input.
	props.UnboundCols = projectionProps.UnboundCols.Difference(inputProps.Relational.OutputCols)
	props.UnboundCols.UnionWith(inputProps.UnboundCols)

	return &props
}
