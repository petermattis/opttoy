package opt

import (
	"fmt"
)

//go:generate optgen -out factory.og.go -pkg opt factory ops/scalar.opt ops/relational.opt ops/enforcer.opt norm/norm.opt norm/filter.opt norm/push_down.opt norm/decorrelate.opt

type Factory struct {
	mem      *memo
	maxSteps int
}

func newFactory(mem *memo, maxSteps int) *Factory {
	f := &Factory{mem: mem, maxSteps: maxSteps}
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

func (f *Factory) onConstruct(group GroupID) GroupID {
	if f.maxSteps <= 0 {
		return group
	}

	e := makeExpr(f.mem, group, defaultPhysPropsID)

	// [HoistScalarSubquery]
	// (Scalar (SubqueryOp $input:* $projection:*) ...)
	// =>
	// (SubqueryOp $input (Scalar $projection ...))
	if e.IsScalar() && e.Operator() != FiltersOp && e.Operator() != ProjectionsOp {
		// Hoist subqueries above scalar expressions. This needs to happen for
		// every input of every scalar expression, so it's easier to do this
		// in code rather than the OptGen language.
		for i := 0; i < e.ChildCount(); i++ {
			child := e.Child(i)
			if child.Operator() == SubqueryOp {
				f.maxSteps--

				// Replace input with the subquery projection child.
				children := e.getChildGroups()
				children[i] = child.ChildGroup(1)

				// Reconstruct the scalar operator with modified children.
				scalar := f.DynamicConstruct(e.Operator(), children, e.privateID())

				// Construct subquery as parent.
				group = f.ConstructSubquery(child.ChildGroup(0), scalar)
				return group
			}
		}
	}

	return group
}

func (f *Factory) commuteInequalityExpr(op Operator, left, right GroupID) GroupID {
	switch op {
	case GeOp:
		return f.ConstructLe(right, left)
	case GtOp:
		return f.ConstructLt(right, left)
	case LeOp:
		return f.ConstructGe(right, left)
	case LtOp:
		return f.ConstructGt(right, left)
	}

	panic(fmt.Sprint("called commuteInequalityExpr with operator %s", op))
}

func (f *Factory) concatFilterConditions(filterLeft, filterRight GroupID) GroupID {
	leftExpr := f.mem.lookupNormExpr(filterLeft)
	if leftExpr.op == TrueOp {
		return filterRight
	} else if leftExpr.op == FalseOp {
		return filterLeft
	}

	rightExpr := f.mem.lookupNormExpr(filterRight)
	if rightExpr.op == TrueOp {
		return filterLeft
	} else if rightExpr.op == FalseOp {
		// TODO(andy): Is it OK to not evaluate the left-side, in case it involves
		//             a side-effect such as an error?
		return filterRight
	}

	leftConditions := leftExpr.asFilters().conditions()
	rightConditions := rightExpr.asFilters().conditions()

	items := make([]GroupID, leftConditions.len, leftConditions.len+rightConditions.len)
	copy(items, f.mem.lookupList(leftConditions))
	items = append(items, f.mem.lookupList(rightConditions)...)

	return f.ConstructFilters(f.StoreList(items))
}

func (f *Factory) flattenFilterCondition(input ListID, filter GroupID) GroupID {
	filterExpr := f.mem.lookupNormExpr(filter)

	var items []GroupID
	if filterExpr.op == AndOp {
		items = make([]GroupID, 0, 2)

		var flatten func(andExpr *andExpr)
		flatten = func(andExpr *andExpr) {
			leftExpr := f.mem.lookupNormExpr(andExpr.left())
			rightExpr := f.mem.lookupNormExpr(andExpr.right())

			if leftExpr.op == AndOp {
				flatten(leftExpr.asAnd())
			} else {
				items = append(items, andExpr.left())
			}

			if rightExpr.op == AndOp {
				flatten(rightExpr.asAnd())
			} else {
				items = append(items, andExpr.right())
			}
		}

		flatten(filterExpr.asAnd())
	} else {
		items = []GroupID{filter}
	}

	// TODO(rytaft): adding inferEquivFilters inside flattenFilterCondition may
	// miss some cases.  flattenFilterConditions is only called once to turn a
	// possibly nested select/join predicate into a flattened list. But other
	// patterns may add items to that list later on (e.g. push-down patterns).
	// Those patterns wouldn't go through this code path, and so we wouldn't
	// infer equivalent filters for the newly added expressions.
	return f.inferEquivFilters(input, items)
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

func (f *Factory) appendListItem(list ListID, newItem GroupID) ListID {
	existingList := f.mem.lookupList(list)
	newList := append(existingList, newItem)

	return f.mem.storeList(newList)
}

func (f *Factory) useFilters(filter GroupID) bool {
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

func (f *Factory) removeApply(op Operator, left, right, filter GroupID) GroupID {
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

func (f *Factory) columnProjections(group GroupID) GroupID {
	outputCols := f.mem.lookupGroup(group).logical.Relational.OutputCols
	items := make([]GroupID, 0, outputCols.Len())
	outputCols.ForEach(func(i int) {
		items = append(items, f.ConstructVariable(f.mem.internPrivate(ColumnIndex(i))))
	})

	return f.ConstructProjections(f.mem.storeList(items), f.mem.internPrivate(&outputCols))
}

func (f *Factory) appendColumnProjections(projections, group GroupID) GroupID {
	projectionsExpr := f.mem.lookupNormExpr(projections).asProjections()
	projectionsItems := f.mem.lookupList(projectionsExpr.items())
	projectionsCols := *f.mem.lookupPrivate(projectionsExpr.cols()).(*ColSet)

	// The final output columns are the union of the columns in "projections"
	// with the appended columns.
	appendCols := f.mem.lookupGroup(group).logical.Relational.OutputCols
	outputCols := projectionsCols.Union(appendCols)

	// If no net-new columns are being appended, then no-op.
	if outputCols.Equals(projectionsCols) {
		return projections
	}

	// Start by copying in the existing projection items.
	items := make([]GroupID, len(projectionsItems), outputCols.Len())
	copy(items, projectionsItems)

	// Now append new projection items synthesized from columns in the group
	// expression.
	appendCols.ForEach(func(i int) {
		if !projectionsCols.Contains(i) {
			items = append(items, f.ConstructVariable(f.mem.internPrivate(ColumnIndex(i))))
		}
	})

	return f.ConstructProjections(f.mem.storeList(items), f.mem.internPrivate(&outputCols))
}

// substitute recursively substitutes oldCol with newCol in the given filter
// expression. For example, if oldCol=x and newCol=y, x < 5 will become y < 5.
// The second return value indicates if the expression changed (i.e., if any variables
// in the expression matched oldCol and were substituted with newCol).
//
// TODO(rytaft): In the future, we may want use code generation to generate
// a "replace visitor" to rebuild the tree bottom-up without creating extra nodes.
func (f *Factory) substitute(filter GroupID, oldCol, newCol ColumnIndex) (GroupID, bool) {
	filterExpr := f.mem.lookupNormExpr(filter)

	// Base case: we have a variable expression.  If it matches oldCol, replace
	// it with newCol.
	if filterExpr.op == VariableOp {
		if f.mem.lookupPrivate(filterExpr.asVariable().col()).(ColumnIndex) == oldCol {
			return f.ConstructVariable(f.InternPrivate(newCol)), true
		}
		return filter, false
	}

	// Recursive Case: Perform recursive substitution on each child of the
	// expression.
	e := makeExpr(f.mem, filter, defaultPhysPropsID)
	children := e.getChildGroups()
	changed, childChanged := false, false
	for i := 0; i < e.ChildCount(); i++ {
		children[i], childChanged = f.substitute(children[i], oldCol, newCol)
		changed = changed || childChanged
	}

	if changed {
		return f.DynamicConstruct(e.Operator(), children, e.privateID()), true
	}
	return filter, false
}

// substituteAll performs all-pairs substitution of the filter columns with
// equivalent columns.
func (f *Factory) substituteAll(filter GroupID, filterCols, equivCols ColSet) []GroupID {
	var items []GroupID
	filterCols.ForEach(func(i int) {
		equivCols.ForEach(func(j int) {
			if i != j {
				item, changed := f.substitute(filter, ColumnIndex(i), ColumnIndex(j))
				if changed {
					items = append(items, item)
				}
			}
		})
	})
	return items
}

// inferEquivFilters augments the original list of filters with new filters
// on equivalent columns.  For example, if columns x and y are equivalent
// and the original list contains (x < 5), the new list will contain
// (x < 5, y < 5).
//
// This function is useful for pushing filters below a join condition. For
// example, consider the following query:
//
//   SELECT * FROM a JOIN b ON a.x = b.y WHERE a.x < 5
//
// By inferring a filter on b.y < 5, this can be rewritten as:
//
//   SELECT * FROM (SELECT * FROM a WHERE a.x < 5) AS a
//            JOIN (SELECT * FROM b WHERE b.y < 5) AS b ON a.x = b.y
func (f *Factory) inferEquivFilters(input ListID, filters []GroupID) GroupID {
	// Start by copying in the existing filters.
	items := make([]GroupID, len(filters))
	copy(items, filters)

	// Find all the equivalent column groups.
	inputList := f.mem.lookupList(input)
	var equivColSets ColSets
	for _, group := range inputList {
		equivColSets = append(equivColSets, f.mem.lookupGroup(group).logical.Relational.EquivCols...)
	}

	// Create new filters by substituting equivalent columns for the existing
	// filter columns.
	for _, filter := range filters {
		filterCols := f.mem.lookupGroup(filter).logical.UnboundCols
		for _, equivCols := range equivColSets {
			if filterCols.Intersects(equivCols) {
				items = append(items, f.substituteAll(filter, filterCols.Intersection(equivCols), equivCols)...)
			}
		}
	}

	return f.ConstructFilters(f.StoreList(items))
}

func (f *Factory) projectsSameCols(projections, input GroupID) bool {
	projectionsExpr := f.mem.lookupNormExpr(projections).asProjections()
	projectionsCols := *f.mem.lookupPrivate(projectionsExpr.cols()).(*ColSet)
	inputCols := f.mem.lookupGroup(input).logical.Relational.OutputCols
	return projectionsCols.Equals(inputCols)
}
