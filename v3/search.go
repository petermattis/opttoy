package v3

import (
	"container/heap"
)

// Search is modelled as a series of tasks that optimize an
// expression. Conceptually, the tasks form a dependency tree very much like
// the dependency tree formed by tools like make. The current implementation of
// dependencies is implicit. [TODO(peter): need to fix this]
//
// Search begins with optimization of the group for the root expression.
//
//   1. optimizeGroup: implements the group (via implementGroup) which
//      generates implementations for the expressions in the group, then
//      selects the plan with the least estimated cost.
//
//   2. optimizeGroupExpr: optimizes all of the child groups (via
//      optimizeGroup), then selects the plan rooted at the group expression
//      with the least estimated cost.
//
//   3. implementGroup: explores the group (via exploreGroup) which generates
//      more logical expressions in the group, then generates implementations
//      for all of the logical expressions.
//
//   4. implementGroupExpr: implements all of the child groups (via
//      implementGroup), then applies any applicable implementation
//      transformations to the expression.
//
//   5. exploreGroup: explores each expression in the group
//
//   6. exploreGroupExpr: explores all of the child groups (via exploreGroup),
//      then applies any applicable exploration transformations to the
//      expression.
//
//   7. transform: applies a transform to the forest of expressions rooted at a
//      particular group expression.

type searchTask struct {
	fn       func()
	parent   *searchTask
	deps     int
	priority int
	sequence int
	index    int
}

func newSearchTask(s *search, parent *searchTask, fn func()) *searchTask {
	t := &searchTask{
		fn:       fn,
		parent:   nil,
		sequence: s.sequence,
		index:    -1,
	}
	s.sequence++
	if parent != nil {
		parent.addChild(t)
	}
	return t
}

func (t *searchTask) addChild(child *searchTask) {
	if child.parent != nil {
		fatalf("task already has parent")
	}
	child.parent = t
	t.deps++
}

type searchQueue struct {
	tasks []*searchTask
}

func (q *searchQueue) Len() int {
	return len(q.tasks)
}

func (q *searchQueue) Swap(i, j int) {
	q.tasks[i], q.tasks[j] = q.tasks[j], q.tasks[i]
	q.tasks[i].index = i
	q.tasks[j].index = j
}

func (q *searchQueue) Less(i, j int) bool {
	qi, qj := q.tasks[i], q.tasks[j]
	if qi.priority == qj.priority {
		return qi.sequence < qj.sequence
	}
	return qi.priority > qj.priority
}

func (q *searchQueue) Push(x interface{}) {
	t := x.(*searchTask)
	t.index = len(q.tasks)
	q.tasks = append(q.tasks, t)
}

func (q *searchQueue) Pop() interface{} {
	n := len(q.tasks)
	t := q.tasks[n-1]
	t.index = -1
	q.tasks = q.tasks[:n-1]
	return t
}

func (q *searchQueue) push(t *searchTask) {
	heap.Push(q, t)
}

func (q *searchQueue) pop() *searchTask {
	return heap.Pop(q).(*searchTask)
}

type search struct {
	memo     *memo
	runnable searchQueue
	sequence int
}

func newSearch(memo *memo) *search {
	return &search{
		memo: memo,
	}
}

func (s *search) run() {
	s.exploreGroup(s.memo.groups[s.memo.root], nil)

	// Run tasks until there is nothing left to do.
	for s.runnable.Len() > 0 {
		t := s.runnable.pop()
		if t.deps > 0 {
			fatalf("%d unfinished deps", t.deps)
		}
		t.fn()
		if t.parent != nil {
			t.parent.deps--
			s.schedule(t.parent)
		}
	}
}

func (s *search) schedule(t *searchTask) {
	if t == nil {
		return
	}
	if t.deps == 0 {
		s.runnable.push(t)
	}
}

func (s *search) optimizeGroup(g *memoGroup, parent *searchTask) {
	s.implementGroup(g, parent)
}

func (s *search) implementGroup(g *memoGroup, parent *searchTask) {
	if g == nil {
		return
	}

	s.exploreGroup(g, parent)
	exprs := g.exprs[g.implemented:]
	g.implemented = int32(len(g.exprs))

	for _, expr := range exprs {
		s.schedule(s.implementGroupExpr(expr, parent))
	}
}

func (s *search) implementGroupExpr(e *memoExpr, parent *searchTask) *searchTask {
	t := newSearchTask(s, parent, func() {
		s.scheduleImplementationTransforms(e, parent)
	})

	// Explore children groups.
	for _, c := range e.children {
		s.implementGroup(s.memo.groups[c], t)
	}
	return t
}

func (s *search) exploreGroup(g *memoGroup, parent *searchTask) {
	if g == nil {
		return
	}

	exprs := g.exprs[g.explored:]
	g.explored = int32(len(g.exprs))

	for _, expr := range exprs {
		s.schedule(s.exploreGroupExpr(expr, parent))
	}
}

func (s *search) exploreGroupExpr(e *memoExpr, parent *searchTask) *searchTask {
	if len(e.children) == 0 {
		s.scheduleExplorationTransforms(e, parent)
		return nil
	}

	t := newSearchTask(s, parent, func() {
		s.scheduleExplorationTransforms(e, parent)
	})

	// Explore children groups.
	for _, c := range e.children {
		s.exploreGroup(s.memo.groups[c], t)
	}
	return t
}

func (s *search) scheduleExplorationTransforms(e *memoExpr, parent *searchTask) {
	for _, xid := range explorationXforms[e.op] {
		s.schedule(s.transform(e, xid, parent))
	}
}

func (s *search) scheduleImplementationTransforms(e *memoExpr, parent *searchTask) {
	for _, xid := range implementationXforms[e.op] {
		s.schedule(s.transform(e, xid, parent))
	}
}

func (s *search) transform(e *memoExpr, xid xformID, parent *searchTask) *searchTask {
	t := newSearchTask(s, parent, func() {
		xform := xforms[xid]
		pattern := xform.pattern()
		var results []*expr

		for cursor := (*expr)(nil); ; {
			cursor = s.memo.bind(e, pattern, cursor)
			if cursor == nil {
				break
			}
			if !xform.check(cursor) {
				continue
			}
			results = xform.apply(cursor, results)
		}

		for _, r := range results {
			// The group that the top-level expressions get added back to is required
			// to be the source group.
			r.loc = memoLoc{e.loc.group, -1}
			s.memo.addExpr(r)
			// Adding the expressions back to the memo might create more groups or
			// add expressions to existing groups. Schedule the source group for
			// exploration again.
			s.exploreGroup(s.memo.groups[e.loc.group], parent)
		}
	})
	return t
}
