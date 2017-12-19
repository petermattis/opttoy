package v4

import (
	"math"
)

const (
	fullyExploredPass optimizePass = math.MaxUint16
)

// explorer ...
// The explorer only traverses expression trees to the depth of the exploration
// patterns. It expects the optimizer to call exploreGroup for each group that
// needs to be explored. The optimizer can then use branch and bound pruning
// to skip exploration of entire sub-trees.
type explorer struct {
	memo    *memo
	factory *factory
}

func newExplorer(memo *memo, factory *factory) *explorer {
	return &explorer{memo: memo, factory: factory}
}

func (e *explorer) exploreGroup(mgrp *memoGroup, pass optimizePass, iter int) (fullyExplored bool) {
	// Do nothing if this group has already been explored during the current
	// optimization pass.
	if e.isGroupExploredThisPass(mgrp, pass, iter) {
		return e.isGroupFullyExplored(mgrp)
	}

	mgrp.exploreCtx.start = mgrp.exploreCtx.end
	mgrp.exploreCtx.end = uint32(len(mgrp.exprs))

	// If this group has already been explored during this optimization pass,
	// then it's not necessary to re-explore expressions that have already been
	// explored this pass, because child groups will not have changed after the
	// first iteration.
	exprs := mgrp.exprs
	if iter > 1 {
		exprs = exprs[mgrp.exploreCtx.start:]
	}

	fullyExplored = true
	for index, offset := range exprs {
		if e.isExprFullyExplored(mgrp, index) {
			continue
		}

		mexpr := e.memo.lookupExpr(offset)
		partlyExplored := index < int(mgrp.exploreCtx.start)

		if e.exploreExpr(mexpr, partlyExplored) {
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
	mgrp.exploreCtx.iter = iter
	return false
}

func (_e *explorer) exploreExpr(mexpr *memoExpr, partlyExplored bool) (fullyExplored bool) {
	switch mexpr.op {
	case innerJoinOp:
		return _e.exploreInnerJoin(mexpr.asInnerJoin(), partlyExplored)

	default:
		fatalf("unhandled op type: %s", mexpr.op)
	}
}

func (_e *explorer) exploreInnerJoin(_root *innerJoinExpr, partlyExplored bool) (fullyExplored bool) {
	fullyExplored = true

	_leftGroup := _e.memo.lookupGroup(_root.left)

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
	//	        (And $newLowerFilter $newLowerFilter2)
	//      )
	//	    $s
	//	    (And $newUpperFilter $newUpperFilter2)
	//  )
	{
		t := _root.right
		upperFilter := _root.filter

		if !_e.exploreGroup(_leftGroup) {
			fullyExplored = false
		}

		for _, _leftOffset := range _e.lookupExploreExprs(_leftGroup, partlyExplored) {
			_innerJoinExpr2 := _e.memo.lookupExpr(_leftOffset).asInnerJoin()
			if _innerJoinExpr2 != nil {
				r := _innerJoinExpr2.left
				s := _innerJoinExpr2.right
				lowerFilter := _innerJoinExpr2.filter

				if _e.canBeSplitByColUsage(upperFilter, s) {
					newLowerFilter, newUpperFilter := _e.splitByColUsage(upperFilter, s)
					newLowerFilter2, newUpperFilter2 := _e.splitByColUsage(lowerFilter, s)

					_and := _e.factory.constructAnd(newLowerFilter, newLowerFilter2)
					_innerJoin := _e.factory.constructInnerJoin(r, t, _and)
					_and2 := _e.factory.constructAnd(newUpperFilter, newUpperFilter2)
					_innerJoinExpr2 := &innerJoinExpr{group: _root.group, left: _innerJoin, right: s, filter: _and2}
					_e.memo.memoizeInnerJoin(_innerJoinExpr2)
				}
			}
		}
	}

	return fullyExplored
}

func (e *explorer) isGroupExploredThisPass(mgrp *memoGroup, pass optimizePass, iter int) bool {
	return mgrp.exploreCtx.pass >= pass && mgrp.exploreCtx.iter >= iter
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

func (e *explorer) canBeSplitByColUsage(filter groupID, group groupID) bool {
	return false
}

func (e *explorer) splitByColUsage(filter groupID, group groupID) (groupID, groupID) {
	return 0, 0
}
