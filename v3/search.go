package v3

import (
	"container/heap"
)

// Search is modelled as a series of tasks that optimize an
// expression. Conceptually, the tasks form a dependency tree very much like
// the dependency tree formed by tools like make. Each task has a count of its
// unfinished dependencies (searchTask.deps) and a pointer to its parent
// task. A task is runnable if it has 0 unfinished dependencies. After a task
// is run, it decrements its parent tasks and schedules it for execution if it
// was the last dependency.
//
// Search begins with optimization of the group for the root expression.
//
//   1. optimizeGroup: implements the group (via implementGroup) which
//      generates implementations for the expressions in the group, then
//      selects the plan with the least estimated cost.
//
//   2. implementGroup: explores the group (via exploreGroup) which generates
//      more logical expressions in the group, then generates implementations
//      for all of the logical expressions.
//
//   3. implementGroupExpr: implements all of the child groups (via
//      implementGroup), then applies any applicable implementation
//      transformations to the expression.
//
//   4. exploreGroup: explores each expression in the group
//
//   5. exploreGroupExpr: explores all of the child groups (via exploreGroup),
//      then applies any applicable exploration transformations to the
//      expression.
//
//   6. transform: applies a transform to the forest of expressions rooted at a
//      particular group expression. There are two flavors of transformation
//      task: exploration transformation and imlementation transformation. The
//      primary difference is the state transition after the task finishes. An
//      exploration transformation recursively schedules exploration of the
//      group it is associated with. An implementation transformation schedules
//      optimization of the inputs.

type taskID int16

const (
	implementGroupExprTask taskID = iota
	implementTransformTask
	exploreGroupExprTask
	exploreTransformTask
)

type searchTask struct {
	parent   *searchTask
	id       taskID
	loc      memoLoc // pointer to memo expression or memo group
	xid      xformID // transformation ID for transform tasks
	deps     int32   // number of unfinished dependent tasks
	priority int32   // priority of this task
	sequence int32   // sequence number for breaking priority ties
	index    int32   // priority queue heap index (see searchQueue)
}

func newSearchTask(s *search, parent *searchTask) *searchTask {
	t := &searchTask{
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

func (t *searchTask) run(s *search) {
	switch t.id {
	case implementGroupExprTask:
		s.implementGroupExpr(t.loc, t.parent)
	case implementTransformTask:
		s.applyTransform(t.loc, t.xid, t.parent, t.id)
	case exploreGroupExprTask:
		s.exploreGroupExpr(t.loc, t.parent)
	case exploreTransformTask:
		s.applyTransform(t.loc, t.xid, t.parent, t.id)
	}

	if t.parent != nil {
		t.parent.deps--
		s.schedule(t.parent)
	}
}

type searchQueue struct {
	tasks []*searchTask
}

func (q *searchQueue) Len() int {
	return len(q.tasks)
}

func (q *searchQueue) Swap(i, j int) {
	q.tasks[i], q.tasks[j] = q.tasks[j], q.tasks[i]
	q.tasks[i].index = int32(i)
	q.tasks[j].index = int32(j)
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
	t.index = int32(len(q.tasks))
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
	sequence int32
}

func newSearch(memo *memo) *search {
	return &search{
		memo: memo,
	}
}

func (s *search) run() {
	s.optimizeGroup(s.memo.groups[s.memo.root], nil)

	// Run tasks until there is nothing left to do.
	for s.runnable.Len() > 0 {
		t := s.runnable.pop()
		if t.deps > 0 {
			fatalf("runnable task with %d unfinished deps", t.deps)
		}
		t.run(s)
	}
}

func (s *search) schedule(t *searchTask) {
	if t != nil && t.deps == 0 {
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

	for _, e := range exprs {
		t := newSearchTask(s, parent)
		t.id = implementGroupExprTask
		t.loc = e.loc

		// Implement children groups.
		for _, c := range e.children {
			s.implementGroup(s.memo.groups[c], t)
		}

		s.schedule(t)
	}
}

func (s *search) implementGroupExpr(loc memoLoc, parent *searchTask) {
	e := s.memo.groups[loc.group].exprs[loc.expr]
	for _, xid := range implementationXforms[e.op] {
		t := newSearchTask(s, parent)
		t.id = implementTransformTask
		t.loc = e.loc
		t.xid = xid
		s.schedule(t)
	}
}

func (s *search) exploreGroup(g *memoGroup, parent *searchTask) {
	if g == nil {
		return
	}

	exprs := g.exprs[g.explored:]
	g.explored = int32(len(g.exprs))

	for _, e := range exprs {
		t := newSearchTask(s, parent)
		t.id = exploreGroupExprTask
		t.loc = e.loc

		// Explore children groups.
		for _, c := range e.children {
			s.exploreGroup(s.memo.groups[c], t)
		}

		s.schedule(t)
	}
}

func (s *search) exploreGroupExpr(loc memoLoc, parent *searchTask) {
	e := s.memo.groups[loc.group].exprs[loc.expr]
	for _, xid := range explorationXforms[e.op] {
		t := newSearchTask(s, parent)
		t.id = exploreTransformTask
		t.loc = e.loc
		t.xid = xid
		s.schedule(t)
	}
}

func (s *search) applyTransform(loc memoLoc, xid xformID, parent *searchTask, id taskID) {
	g := s.memo.groups[loc.group]
	e := g.exprs[loc.expr]
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

	if len(results) > 0 {
		for _, r := range results {
			// The group that the top-level expressions get added back to is required
			// to be the source group.
			r.loc = memoLoc{e.loc.group, -1}
			s.memo.addExpr(r)
		}

		// Adding the expressions back to the memo might create more groups or
		// add expressions to existing groups. Schedule the source group for
		// exploration again.
		switch id {
		case implementTransformTask:
			// TODO(peter): should this be optimizeInputs?
			s.implementGroup(g, parent)
		case exploreTransformTask:
			s.exploreGroup(g, parent)
		default:
			fatalf("unexpected task id %d", id)
		}
	}
}
