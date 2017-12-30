package opt

//go:generate optgen -out factory.og.go -pkg opt factory ops/scalar.opt ops/relational.opt ops/enforcer.opt norm/norm.opt norm/decorrelate.opt norm/filter.opt

type Factory struct {
	mem      *memo
	maxSteps int

	// The customNormalize function cannot be directly invoked in generated
	// code due to golang initialization loop rules, so do it indirectly.
	onConstruct func(fingerprint fingerprint, group GroupID) GroupID
}

func newFactory(mem *memo, maxSteps int) *Factory {
	f := &Factory{mem: mem, maxSteps: maxSteps}
	f.onConstruct = f.normalizeManually
	return f
}

func (f *Factory) Metadata() *Metadata {
	return f.mem.metadata
}

func (f *Factory) StoreList(items []GroupID) ListID {
	return f.mem.storeList(items)
}

func (f *Factory) InternPrivate(private interface{}) PrivateID {
	return f.mem.internPrivate(private)
}

func (f *Factory) normalizeManually(fingerprint fingerprint, group GroupID) GroupID {
	if f.maxSteps <= 0 {
		return group
	}

	e := makeExpr(f.mem, group, defaultPhysPropsID)

	if e.IsScalar() && e.Operator() != FilterListOp {
		// Hoist subqueries above scalar expressions. This needs to happen for
		// every input of every scalar expression, so it's easier to do this
		// manually.
		for i := 0; i < e.ChildCount(); i++ {
			child := e.Child(i)
			if child.Operator() == SubqueryOp && f.hasUnboundCols(child.group) {
				f.maxSteps--

				// Replace input with the subquery projection child.
				children := e.getChildGroups()
				children[i] = child.ChildGroup(1)

				// Reconstruct the scalar operator with modified children.
				scalar := f.dynamicConstruct(e.Operator(), children, e.privateID())

				// Construct subquery as parent.
				group = f.ConstructSubquery(child.ChildGroup(0), scalar)
				f.mem.addAltFingerprint(fingerprint, group)
				return group
			}
		}
	}

	return group
}

func (f *Factory) concatFilterConditions(filterLeft, filterRight GroupID) GroupID {
	leftConditions := f.mem.lookupNormExpr(filterLeft).asFilterList().conditions
	rightConditions := f.mem.lookupNormExpr(filterRight).asFilterList().conditions

	items := make([]GroupID, leftConditions.len, leftConditions.len+rightConditions.len)
	copy(items, f.mem.lookupList(leftConditions))
	items = append(items, f.mem.lookupList(rightConditions)...)

	return f.ConstructFilterList(f.StoreList(items))
}

func (f *Factory) flattenFilterCondition(filter GroupID) GroupID {
	filterExpr := f.mem.lookupNormExpr(filter)

	var items []GroupID
	if filterExpr.op == AndOp {
		items = make([]GroupID, 0, 2)

		var flatten func(andExpr *andExpr)
		flatten = func(andExpr *andExpr) {
			leftExpr := f.mem.lookupNormExpr(andExpr.left)
			rightExpr := f.mem.lookupNormExpr(andExpr.right)

			if leftExpr.op == AndOp {
				flatten(leftExpr.asAnd())
			} else {
				items = append(items, andExpr.left)
			}

			if rightExpr.op == AndOp {
				flatten(rightExpr.asAnd())
			} else {
				items = append(items, andExpr.right)
			}
		}

		flatten(filterExpr.asAnd())
	} else {
		items = []GroupID{filter}
	}

	return f.ConstructFilterList(f.StoreList(items))
}

func (f *Factory) isLowerExpr(left, right GroupID) bool {
	return left < right
}

func (f *Factory) removeListItem(list ListID, search GroupID) ListID {
	existingList := f.mem.lookupList(list)
	newList := make([]GroupID, len(existingList)-1)

	for i, item := range existingList {
		if item == search {
			newList = append(newList[:i], existingList[i+1:]...)
			break
		}

		newList[i] = item
	}

	return f.mem.storeList(newList)
}

func (f *Factory) replaceListItem(list ListID, search, replace GroupID) ListID {
	existingList := f.mem.lookupList(list)
	newList := make([]GroupID, len(existingList))

	for i, item := range existingList {
		if item == search {
			newList[i] = replace
		} else {
			newList[i] = item
		}
	}

	return f.mem.storeList(newList)
}

func (f *Factory) useFilterList(filter GroupID) bool {
	switch f.mem.lookupNormExpr(filter).op {
	case TrueOp, FalseOp:
		return false
	}

	return true
}

func (f *Factory) isEmptyList(list ListID) bool {
	return list.isEmpty()
}

func (f *Factory) isCorrelated(this, that GroupID) bool {
	thisGroup := f.mem.lookupGroup(this)
	thatGroup := f.mem.lookupGroup(that)
	return thisGroup.logical.UnboundCols.Intersects(thatGroup.logical.Relational.OutputCols)
}

func (f *Factory) hasUnboundCols(rel GroupID) bool {
	return !f.mem.lookupGroup(rel).logical.UnboundCols.Empty()
}

func (f *Factory) singleColIndex(rel GroupID) PrivateID {
	cols := f.mem.lookupGroup(rel).logical.Relational.OutputCols
	index, _ := cols.Next(0)
	return f.mem.internPrivate(ColumnIndex(index))
}

func (f *Factory) nonJoinApply(op Operator, left, right, filter GroupID) GroupID {
	switch op {
	case InnerJoinApplyOp:
		return f.ConstructInnerJoin(left, right, filter)
	case LeftJoinApplyOp:
		return f.ConstructLeftJoin(left, right, filter)
	case RightJoinApplyOp:
		return f.ConstructRightJoin(left, right, filter)
	case FullJoinApplyOp:
		return f.ConstructFullJoin(left, right, filter)
	case SemiJoinApplyOp:
		return f.ConstructSemiJoin(left, right, filter)
	case AntiJoinApplyOp:
		return f.ConstructAntiJoin(left, right, filter)
	}

	fatalf("unexpected join operator: %v", op)
	return 0
}
