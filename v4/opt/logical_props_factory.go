package opt

import (
	"fmt"
)

type logicalPropsFactory struct {
	mem *memo
}

func (f *logicalPropsFactory) init(mem *memo) {
	f.mem = mem
}

func (f *logicalPropsFactory) constructProps(e *Expr) *LogicalProps {
	if e.IsScalar() {
		var props LogicalProps
		f.setScalarUnboundCols(&props, e)
		return &props
	}

	switch e.Operator() {
	case ScanOp:
		return f.constructScanProps(e)
	case ValuesOp:
		return &LogicalProps{}
	case ProjectOp:
		return f.constructProjectProps(e)

	case LeftJoinOp:
		return f.constructJoinProps(e)
	case RightJoinOp:
		return f.constructJoinProps(e)
	case FullJoinOp:
		return f.constructJoinProps(e)
	case SemiJoinOp:
		return f.constructJoinProps(e)
	case AntiJoinOp:
		return f.constructJoinProps(e)
	case InnerJoinApplyOp:
		return f.constructJoinProps(e)
	case LeftJoinApplyOp:
		return f.constructJoinProps(e)
	case RightJoinApplyOp:
		return f.constructJoinProps(e)
	case FullJoinApplyOp:
		return f.constructJoinProps(e)
	case SemiJoinApplyOp:
		return f.constructJoinProps(e)
	case AntiJoinApplyOp:
		return f.constructJoinProps(e)
	}

	panic(fmt.Sprintf("unrecognized expression type: %v", e.op))
}

func (f *logicalPropsFactory) constructJoinProps(e *Expr) *LogicalProps {
	var props LogicalProps

	leftProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	rightProps := f.mem.lookupGroup(e.ChildGroup(1)).logical
	filterProps := f.mem.lookupGroup(e.ChildGroup(2)).logical

	// Output columns are union of columns from left and right inputs.
	props.Relational.OutputCols.UnionWith(leftProps.Relational.OutputCols)
	props.Relational.OutputCols.UnionWith(rightProps.Relational.OutputCols)

	// Left/full outer joins can result in right columns becoming null.
	// Otherwise, propagate not null setting from right child.
	switch e.Operator() {
	case LeftJoinOp, FullJoinOp, LeftJoinApplyOp, FullJoinApplyOp:
		props.Relational.NotNullCols.UnionWith(rightProps.Relational.OutputCols)

	default:
		props.Relational.NotNullCols.UnionWith(rightProps.Relational.NotNullCols)
	}

	// Right/full outer joins can result in left columns becoming null.
	// Otherwise, propagate not null setting from left child.
	switch e.Operator() {
	case RightJoinOp, FullJoinOp, RightJoinApplyOp, FullJoinApplyOp:
		props.Relational.NotNullCols.UnionWith(leftProps.Relational.OutputCols)

	default:
		props.Relational.NotNullCols.UnionWith(leftProps.Relational.NotNullCols)
	}

	// Any columns which are used by any of the join children, but are not
	// part of the join output columns are unbound columns.
	props.UnboundCols = filterProps.UnboundCols.Difference(props.Relational.OutputCols)
	props.UnboundCols.UnionWith(leftProps.UnboundCols)
	props.UnboundCols.UnionWith(rightProps.UnboundCols)

	// Union overlapping equivalent columns from inputs.
	props.addEquivColumnSets(leftProps.Relational.EquivCols)
	props.addEquivColumnSets(rightProps.Relational.EquivCols)

	// Set additional properties according to the join filter.
	filter := e.Child(2)
	f.setPropsFromFilter(&props, &filter)

	return &props
}

func (f *logicalPropsFactory) constructScanProps(e *Expr) *LogicalProps {
	var props LogicalProps

	tblIndex := e.Private().(TableIndex)
	tbl := f.mem.metadata.Table(tblIndex)

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

func (f *logicalPropsFactory) constructProjectProps(e *Expr) *LogicalProps {
	var props LogicalProps

	inputProps := f.mem.lookupGroup(e.ChildGroup(0)).logical
	projectionProps := f.mem.lookupGroup(e.ChildGroup(1)).logical

	// Output columns include both the projection list columns *and* the input
	// columns, even if the projection list excludes columns. Those "hidden"
	// columns are excluded by physical properties rather than by logical.
	props.Relational.OutputCols = inputProps.Relational.OutputCols.Copy()
	projectionList := f.mem.lookupNormExpr(e.ChildGroup(1)).asProjections()
	projectionCols := f.mem.lookupPrivate(projectionList.cols).(*ColSet)
	props.Relational.OutputCols.UnionWith(*projectionCols)

	// Inherit not null columns from input.
	props.Relational.NotNullCols = inputProps.Relational.NotNullCols

	// Any columns which are used by any of the project children, but are not
	// part of the output columns are unbound columns.
	props.UnboundCols = projectionProps.UnboundCols.Difference(props.Relational.OutputCols)
	props.UnboundCols.UnionWith(inputProps.UnboundCols)

	// Inherit equivalent columns from input.
	props.Relational.EquivCols = inputProps.Relational.EquivCols

	return &props
}

func (f *logicalPropsFactory) setScalarUnboundCols(props *LogicalProps, e *Expr) {
	for i := 0; i < e.ChildCount(); i++ {
		props.UnboundCols.UnionWith(f.mem.lookupGroup(e.ChildGroup(i)).logical.UnboundCols)
	}

	return
}

// Add additional not-NULL columns based on the filtering expression.
func (f *logicalPropsFactory) setPropsFromFilter(props *LogicalProps, filter *Expr) {
	// Expand the set of non-NULL columns based on the filter.
	//
	// TODO(peter): Need to make sure the filter is not null-tolerant.
	props.Relational.NotNullCols.UnionWith(filter.Logical().UnboundCols)

	f.setEquivProperties(props, filter)
}

func (f *logicalPropsFactory) setEquivProperties(props *LogicalProps, filter *Expr) {
	// Find equivalent columns.
	switch filter.Operator() {
	case EqOp:
		left := filter.Child(0)
		right := filter.Child(1)

		if left.op == VariableOp && right.op == VariableOp {
			cols := left.Logical().UnboundCols
			cols.UnionWith(right.Logical().UnboundCols)
			props.addEquivColumns(cols)
		}

	case AndOp:
		left := filter.Child(0)
		right := filter.Child(1)

		f.setEquivProperties(props, &left)
		f.setEquivProperties(props, &right)
	}

	// TODO(peter): Support tuple comparisons such as "(a, b) = (c, d)".
}
