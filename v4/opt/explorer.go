package opt

import (
	"math"
)

var fullyExploredPass = optimizePass{major: math.MaxInt16, minor: math.MaxInt16}

// explorer ...
// The explorer only traverses expression trees to the depth of the exploration
// patterns. It expects the optimizer to call exploreGroup for each group that
// needs to be explored. The optimizer can then use branch and bound pruning
// to skip exploration of entire sub-trees.
type explorer struct {
	mem     *memo
	factory *Factory
}

func (e *explorer) init(factory *Factory) {
	e.mem = factory.mem
	e.factory = factory
}

func (e *explorer) exploreGroup(mgrp *memoGroup, pass optimizePass) (fullyExplored bool) {
	// Do nothing if this group has already been explored during the current
	// optimization pass.
	if e.isGroupExploredThisPass(mgrp, pass) {
		return e.isGroupFullyExplored(mgrp)
	}

	mgrp.exploreCtx.start = mgrp.exploreCtx.end
	mgrp.exploreCtx.end = uint32(len(mgrp.exprs))

	// If this group has already been explored during this optimization pass,
	// then it's not necessary to re-explore expressions that have already been
	// explored this pass, because child groups will not have changed after the
	// first iteration.
	exprs := mgrp.exprs
	if pass.minor > 1 {
		exprs = exprs[mgrp.exploreCtx.start:]
	}

	fullyExplored = true
	for index, offset := range exprs {
		if e.isExprFullyExplored(mgrp, index) {
			continue
		}

		mexpr := e.mem.lookupExpr(offset)
		partlyExplored := index < int(mgrp.exploreCtx.start)

		if e.exploreExpr(mexpr, pass, partlyExplored) {
			e.markExprAsFullyExplored(mgrp, index)
		} else {
			fullyExplored = false
		}
	}

	if fullyExplored {
		mgrp.exploreCtx.pass = fullyExploredPass
		return true
	}

	mgrp.exploreCtx.pass = pass
	return false
}

func (_e *explorer) exploreExpr(base *memoExpr, pass optimizePass, partlyExplored bool) (fullyExplored bool) {
	switch base.op {
	case InnerJoinOp:
		return _e.exploreInnerJoin(base.asInnerJoin(), pass, partlyExplored)

	default:
		fatalf("unhandled op type: %s", base.op)
		return false
	}
}

func (_e *explorer) exploreInnerJoin(_root *innerJoinExpr, pass optimizePass, partlyExplored bool) (fullyExplored bool) {
	fullyExplored = true

	//_leftGroup := _e.mem.lookupGroup(_root.left)

	//	[AssociateJoin]
	//	(InnerJoin
	//	    (InnerJoin $r:* $s:* $lowerFilter:*)
	//	    $t:*
	//	    $upperFilter:* && (CanBeSplitByColUsage $upperFilter $s)
	//  )
	//	=>
	//	($newLowerFilter, $newUpperFilter):(SplitByColUsage $upperFilter $s)
	//	($newLowerFilter2, $newUpperFilter2):(SplitByColUsage $lowerFilter $s)
	//	(InnerJoin
	//	    (InnerJoin
	//	        $r
	//      	$t
	//	        (FilterList [$newLowerFilter $newLowerFilter2])
	//      )
	//	    $s
	//	    (FilterList [$newUpperFilter $newUpperFilter2])
	//  )
	/*	{
		t := _root.right
		upperFilter := _root.filter

		if !_e.exploreGroup(_leftGroup, pass) {
			fullyExplored = false
		}

		for _, _leftOffset := range _e.lookupExploreExprs(_leftGroup, partlyExplored) {
			_innerJoinExpr2 := _e.mem.lookupExpr(_leftOffset).asInnerJoin()
			if _innerJoinExpr2 != nil {
				r := _innerJoinExpr2.left
				s := _innerJoinExpr2.right
				lowerFilter := _innerJoinExpr2.filter

				if _e.canBeSplitByColUsage(upperFilter, s) {
					newLowerFilter, newUpperFilter := _e.splitByColUsage(upperFilter, s)
					newLowerFilter2, newUpperFilter2 := _e.splitByColUsage(lowerFilter, s)

					_filter := _e.factory.ConstructFilterList(newLowerFilter, newLowerFilter2)
					_innerJoin := _e.factory.ConstructInnerJoin(r, t, _and)
					_filter2 := _e.factory.ConstructFilterList(newUpperFilter, newUpperFilter2)
					_innerJoinExpr2 := innerJoinExpr{memoExpr: memoExpr{op: InnerJoinOp, group: _root.group}, left: _innerJoin, right: s, filter: _and2}
					_e.mem.memoizeInnerJoin(&_innerJoinExpr2)
				}
			}
		}
	}*/

	return fullyExplored
}

func (e *explorer) isGroupExploredThisPass(mgrp *memoGroup, pass optimizePass) bool {
	return !mgrp.exploreCtx.pass.Less(pass)
}

func (e *explorer) isGroupFullyExplored(mgrp *memoGroup) bool {
	return mgrp.exploreCtx.pass == fullyExploredPass
}

func (e *explorer) isExprFullyExplored(mgrp *memoGroup, index int) bool {
	return mgrp.exploreCtx.exprs.Contains(index)
}

func (e *explorer) markExprAsFullyExplored(mgrp *memoGroup, index int) {
	mgrp.exploreCtx.exprs.Add(index)
}

func (e *explorer) lookupExploreExprs(mgrp *memoGroup, skipPartlyExplored bool) []exprOffset {
	if skipPartlyExplored {
		return mgrp.exprs[mgrp.exploreCtx.start:mgrp.exploreCtx.end]
	}

	return mgrp.exprs[:mgrp.exploreCtx.end]
}

func (e *explorer) canBeSplitByColUsage(filter GroupID, group GroupID) bool {
	return false
}

func (e *explorer) splitByColUsage(filter GroupID, group GroupID) (GroupID, GroupID) {
	return 0, 0
}
