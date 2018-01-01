package opt

type physicalPropsFactory struct {
	mem *memo
}

func (c *physicalPropsFactory) init(mem *memo) {
	c.mem = mem
}

func (c *physicalPropsFactory) canProvide(e *Expr) bool {
	requiredProps := c.mem.lookupPhysicalProps(e.required)

	if requiredProps.Projection.Defined() && !c.canProvideProjection(e) {
		return false
	}

	if requiredProps.Ordering.Defined() && !c.canProvideOrdering(e) {
		return false
	}

	return true
}

func (c *physicalPropsFactory) canProvideProjection(e *Expr) bool {
	// Only the project operator can provide column ordering and naming. Other
	// operators require the ArrangeOp enforcer.
	return e.Operator() == ProjectOp
}

func (c *physicalPropsFactory) canProvideOrdering(e *Expr) bool {
	if e.IsRelational() {
		requiredProps := c.mem.lookupPhysicalProps(e.required)

		switch e.Operator() {
		case ScanOp:
			// Table scans provide primary key ordering.
			tblIndex := e.Private().(TableIndex)
			ordering := c.mem.metadata.Table(tblIndex).Ordering
			return ordering.Provides(requiredProps.Ordering)

		case ValuesOp, SelectOp:
			// Ordering is pass through property for these operators.
			return true

		case ProjectOp:
			// Project can only provide an ordering if it applies only to
			// columns provided by its input.
			outputCols := c.mem.lookupGroup(e.ChildGroup(0)).logical.Relational.OutputCols
			for _, colIndex := range requiredProps.Ordering {
				if !outputCols.Contains(int(colIndex)) {
					return false
				}
			}
			return true

		case InnerJoinOp, LeftJoinOp, RightJoinOp, FullJoinOp,
			SemiJoinOp, AntiJoinOp, InnerJoinApplyOp, LeftJoinApplyOp,
			RightJoinApplyOp, FullJoinApplyOp, SemiJoinApplyOp, AntiJoinApplyOp:
			// Nested loop joins preserve ordering of left child, so ordering
			// property can be passed through if it consists only of columns
			// from the left child.
			leftProps := c.mem.lookupGroup(e.ChildGroup(0)).logical
			for _, colIndex := range requiredProps.Ordering {
				if !leftProps.Relational.OutputCols.Contains(int(colIndex)) {
					return false
				}
			}
			return true
		}
	}

	return false
}

func (c *physicalPropsFactory) constructChildProps(e *Expr, nth int) (required physicalPropsID) {
	if e.IsRelational() {
		switch e.Operator() {
		case ProjectOp:
			return c.constructProjectChildProps(e, nth)

		case SelectOp:
			return c.constructSelectChildProps(e, nth)

		case InnerJoinOp, LeftJoinOp, RightJoinOp, FullJoinOp,
			SemiJoinOp, AntiJoinOp, InnerJoinApplyOp, LeftJoinApplyOp,
			RightJoinApplyOp, FullJoinApplyOp, SemiJoinApplyOp, AntiJoinApplyOp:
			return c.constructJoinChildProps(e, nth)

		case UnionOp, GroupByOp:
			// Pass through for all inputs (provides no properties).
			return e.required

		case SortOp:
			return c.constructSortChildProps(e, nth)

		case ArrangeOp:
			return c.constructArrangeChildProps(e, nth)
		}

		fatalf("unrecognized relational expression type: %v", e.op)
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructProjectChildProps(e *Expr, nth int) physicalPropsID {
	if nth == 0 {
		requiredProps := c.mem.lookupPhysicalProps(e.required)

		if !requiredProps.Projection.Defined() {
			// Fast path - pass through properties.
			return e.required
		}

		var props PhysicalProps

		// 1. Do not require any particular projection ordering or naming from
		//    the input.
		// 2. Pass through the ordering requirement. It must only involve
		//    columns provided by the input, or else the ordering would have
		//    been handled by an enforcer.
		props.Ordering = requiredProps.Ordering

		return c.mem.internPhysicalProps(&props)
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructSelectChildProps(e *Expr, nth int) physicalPropsID {
	if nth == 0 {
		// 1. Projection property is never required or provided.
		// 2. Ordering requirement is always passed through to input.
		return e.required
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructJoinChildProps(e *Expr, nth int) physicalPropsID {
	if nth == 0 || nth == 1 {
		// 1. Projection property is never required or provided.
		// 2. Ordering requirement must involve columns provided by the input,
		//    or else the ordering would have been handled by an enforcer.
		return e.required
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructSortChildProps(e *Expr, nth int) physicalPropsID {
	// Required props of sort input are the same as the parent, minus
	// the ordering property.
	required := *e.mem.lookupPhysicalProps(e.required)
	required.Ordering = nil
	return e.mem.internPhysicalProps(&required)
}

func (c *physicalPropsFactory) constructArrangeChildProps(e *Expr, nth int) physicalPropsID {
	// Required props of arrange input are the same as the parent, minus
	// the projection property.
	required := *e.mem.lookupPhysicalProps(e.required)
	required.Projection = Projection{}
	return e.mem.internPhysicalProps(&required)
}
