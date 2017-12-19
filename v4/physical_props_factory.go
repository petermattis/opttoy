package v4

type physicalPropsFactory struct{}

func (c *physicalPropsFactory) constructProvidedProps(e *expr) physicalPropsID {
	switch e.operator() {
	case selectOp:
		return c.constructSelectProvidedProps(e)
	case sortOp:
		return e.required
	}

	return wildcardPhysPropsID
}

func (c *physicalPropsFactory) constructRequiredProps(e *expr, nth int) physicalPropsID {
	switch e.operator() {
	case selectOp:
		return c.constructSelectRequiredProps(e, nth)
	case sortOp:
		return c.constructSortRequiredProps(e, nth)
	}

	return wildcardPhysPropsID
}

func (c *physicalPropsFactory) constructSelectProvidedProps(e *expr) physicalPropsID {
	input := e.memo.lookupGroup(e.childGroup(0))
	best := &input.bestExprs[e.required]
	return best.provided
}

func (c *physicalPropsFactory) constructSelectRequiredProps(e *expr, nth int) physicalPropsID {
	// Pass-through required properties to input child.
	if nth == 0 {
		return e.required
	}

	return wildcardPhysPropsID
}

func (c *physicalPropsFactory) constructSortRequiredProps(e *expr, nth int) physicalPropsID {
	if nth == 0 {
		required := *e.memo.lookupPhysicalProps(e.required)
		required.order = nil
		return e.memo.internPhysicalProps(&required)
	}

	return wildcardPhysPropsID
}
