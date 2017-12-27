package opt

type physicalPropsFactory struct {
	mem        *memo
	candidates []Expr
}

func (c *physicalPropsFactory) init(mem *memo) {
	c.mem = mem
}

func (c *physicalPropsFactory) generateCandidates(e *Expr) []Expr {
	c.candidates = [:0]

}

func (c *physicalPropsFactory) constructRequiredProps(e *Expr, nth int) physicalPropsID {
	if e.IsRelational() {
		switch e.Operator() {
		case ProjectOp:
			return c.constructProjectRequiredProps(e, nth)
		case SelectOp:
			return c.constructSelectRequiredProps(e, nth)
		case SortOp:
			return c.constructSortRequiredProps(e, nth)
		}
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructProjectRequiredProps(e *Expr, nth int) physicalPropsID {
	if nth == 0 {
		var props PhysicalProps
		requiredProps := c.mem.lookupPhysicalProps(e.required)

		if requiredProps.Projection.Defined() {
			// Need both the required columns and columns used by the
			// projections from the input expression.
			inputProps := e.mem.lookupGroup(e.ChildGroup(0)).logical
			projectionsProps := e.mem.lookupGroup(e.ChildGroup(1)).logical

			props.Projection.unordered.UnionWith(projectionsProps.UnboundCols)
			props.Projection.unordered.IntersectionWith(inputProps.Relational.OutputCols)
		}

		if requiredProps.Ordering.Defined() {
			// Pass-thru the ordering requirement.
			props.Ordering = requiredProps.Ordering
		}

		return c.mem.internPhysicalProps(&props)
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructSelectRequiredProps(e *Expr, nth int) physicalPropsID {
	// Pass-thru required properties to input child.
	if nth == 0 {
		return e.required
	}

	return defaultPhysPropsID
}

func (c *physicalPropsFactory) constructSortRequiredProps(e *Expr, nth int) physicalPropsID {
	if nth == 0 {
		// Required props of sort input are the same as the parent, minus
		// the ordering property.
		required := *e.mem.lookupPhysicalProps(e.required)
		required.Ordering = nil
		return e.mem.internPhysicalProps(&required)
	}

	return defaultPhysPropsID
}
