package v3

import (
	"bytes"
	"fmt"
	"math"
	"sort"
)

// groupID identifies a memo group. Groups have numbers greater than 0; a
// groupID of 0 indicates an empty expression or an unknown group.
type groupID int32

// exprID identifies an expression within its memo group.
type exprID int32

// memoLoc describes the location of an expression in the memo.
type memoLoc struct {
	group groupID
	expr  exprID
}

func (l memoLoc) String() string {
	return fmt.Sprintf("%d.%d", l.group, l.expr)
}

const numInlineChildren = 3

// memoExprFingerprint contains the fingerprint of memoExpr. Two memo
// expressions are considered equal if their fingerprints are equal. The
// fast-path case for expressions with 3 or fewer children and which do not
// contain physical properties or private data is for memoExprFingerprint.extra
// to be empty. In the slow-path case, that extra is initialized to distinguish
// such expressions.
type memoExprFingerprint struct {
	op       operator
	children [numInlineChildren]groupID
	extra    string
}

// memoExpr is a memoized representation of an expression. Unlike expr which
// represents a single expression, a memoExpr roots a forest of
// expressions. This is accomplished by recursively memoizing children and
// storing them in the memo structure. memoExpr.children refers to child groups
// of logically equivalent expressions. Because memoExpr refers to a forest of
// expressions, it is challenging to perform transformations directly upon
// it. Instead, transformations are performed by extracting an expr fragment
// matching a pattern from the memo, performing the transformation and then
// inserting the transformed result back into the memo.
//
// For relational expressions, logical equivalency is defined as equivalent
// group fingerprints (see memoExpr.groupFingerprint()). For scalar
// expressions, logical equivalency is defined as equivalent memoExpr (see
// memoExpr.fingerprint()). While scalar expressions are stored in the memo,
// each scalar expression group contains only a single entry.
type memoExpr struct {
	op operator // expr.op
	// numChildren and childrenBuf combine to represent expr.children. If an
	// expression contains 3 or fewer children they are stored in childrenBuf and
	// numChildren indicates the number of children. If the expression contains
	// more than 3 children, they are stored at memo.children[numChildren-3-1].
	numChildren   int32
	childrenBuf   [numInlineChildren]groupID
	physicalProps int32 // expr.physicalProps
	private       int32 // memo.private[expr.private]
	// NB: expr.{props,scalarProps} are the same for all expressions in the group
	// and stored in memoGroup.
}

func (e *memoExpr) matchOp(pattern *expr) bool {
	return isPatternSentinel(pattern) || pattern.op == e.op
}

func (e *memoExpr) children(m *memo) []groupID {
	if e.numChildren <= numInlineChildren {
		return e.childrenBuf[:e.numChildren]
	}
	return m.children[e.numChildren-numInlineChildren-1]
}

func (e *memoExpr) DebugString(m *memo) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[%s", e.op)

	if e.private > 0 {
		p := m.private[e.private]
		switch t := p.(type) {
		case nil:
		case *table:
			fmt.Fprintf(&buf, " %s", t.name)
		default:
			fmt.Fprintf(&buf, " %s", p)
		}
	}

	if props := m.physicalProps[e.physicalProps]; props != nil {
		if f := props.fingerprint(); f != "" {
			fmt.Fprintf(&buf, " %s", f)
		}
	}

	if len(e.children(m)) > 0 {
		fmt.Fprintf(&buf, " [")
		for i, c := range e.children(m) {
			if i > 0 {
				buf.WriteString(" ")
			}
			if c <= 0 {
				buf.WriteString("-")
			} else {
				fmt.Fprintf(&buf, "%d", c)
			}
		}
		fmt.Fprintf(&buf, "]")
	}
	buf.WriteString("]")
	return buf.String()
}

// fingerprint returns a string which uniquely identifies the expression within
// the context of the memo.
func (e memoExpr) fingerprint(m *memo) memoExprFingerprint {
	return e.commonFingerprint(m, e.physicalProps)
}

func (e memoExpr) groupFingerprint(m *memo) memoExprFingerprint {
	f := e.commonFingerprint(m, 0)
	// TODO(peter): Generalize this normalization. It should probably be operator
	// specific.
	if e.op == innerJoinOp {
		if f.children[0] > f.children[1] {
			f.children[0], f.children[1] = f.children[1], f.children[0]
		}
	}
	return f
}

func (e *memoExpr) commonFingerprint(m *memo, physicalProps int32) memoExprFingerprint {
	var f memoExprFingerprint
	f.op = e.op

	children := e.children(m)
	if len(children) <= numInlineChildren {
		for i := range children {
			f.children[i] = children[i]
		}
	}

	if e.private > 0 || physicalProps > 0 || len(children) > numInlineChildren {
		var buf bytes.Buffer
		p := m.private[e.private]
		switch t := p.(type) {
		case nil:
		case *table:
			fmt.Fprintf(&buf, " %s", t.name)
		default:
			fmt.Fprintf(&buf, " %s", p)
		}

		if props := m.physicalProps[physicalProps]; props != nil {
			if f := props.fingerprint(); f != "" {
				fmt.Fprintf(&buf, " %s", f)
			}
		}

		if len(children) > 0 {
			fmt.Fprintf(&buf, " [")
			for i, c := range e.children(m) {
				if i > 0 {
					buf.WriteString(" ")
				}
				if c <= 0 {
					buf.WriteString("-")
				} else {
					fmt.Fprintf(&buf, "%d", c)
				}
			}
			fmt.Fprintf(&buf, "]")
		}
		f.extra = buf.String()
	}
	return f
}

func (e *memoExpr) info() operatorInfo {
	return operatorTab[e.op]
}

// memoOptState maintains the optimization state for a group for a particular
// optimization context.
type memoOptState struct {
	// The index of the last optimized expression
	optimized exprID
	// The location of the lowest cost expression.
	loc memoLoc
	// The cost of the lowest cost expression.
	cost float32
	// The opt state of children of the lowest expression.
	children []*memoOptState
}

// memoGroup stores a set of logically equivalent expressions. See the comments
// on memoExpr for the definition of logical equivalency.
type memoGroup struct {
	// The ID (a.k.a. index, memoLoc.group) of the group within the memo.
	id groupID
	// The index of the last explored expression. Used by search.
	explored exprID
	// The index of the last implemented expression. Used by search.
	implemented exprID
	// A map from memo expression fingerprint to the index of the memo expression
	// in the exprs slice. Used to determine if a memoExpr already exists in the
	// group.
	exprMap map[memoExprFingerprint]exprID
	exprs   []memoExpr
	// The relational properties for the group. Nil if the group contains scalar
	// expressions.
	props *relationalProps
	// The scalar properties for the group. Nil if the group contains relational
	// expressions.
	scalarProps *scalarProps
	// Map from optimization context (i.e. required physicalProperties)
	// fingerprint to optimization state (the best plan and cost, the children
	// locations associated with that plan, etc).
	//
	// TODO(peter): We intern the physicalProps in memo, which should allow this
	// to be a map[*physicalProps]exprID.
	optMap map[string]*memoOptState
}

func (g *memoGroup) getOptState(required *physicalProps) *memoOptState {
	if g.optMap == nil {
		g.optMap = make(map[string]*memoOptState)
	}
	rf := required.fingerprint()
	opt, ok := g.optMap[rf]
	if !ok {
		opt = &memoOptState{cost: math.MaxFloat32}
		g.optMap[rf] = opt
	}
	return opt
}

type memo struct {
	// A map from expression fingerprint (memoExpr.fingerprint()) to the index of
	// the group the expression resides in.
	exprMap map[memoExprFingerprint]groupID
	// A map from group fingerprint to the index of the group in the groups
	// slice. For relational groups, the fingerprint for a group is the
	// fingerprint of the relational properties
	// (relationalProps.fingerprint()). For scalar groups, the fingerprint for a
	// group is the fingerprint of the memo expression (memoExpr.fingerprint()).
	groupMap map[memoExprFingerprint]groupID
	// The slice of groups, indexed by group ID (i.e. memoLoc.group). Note the
	// group ID 0 is invalid in order to allow zero initialization of expr to
	// indicate an expression that did not originate from the memo.
	groups []memoGroup
	// External storage of child groups for memo expressions which contain more
	// than 3 children. See memoExpr.{numChildren,childrenBuf}.
	children [][]groupID
	// Physical properties attached to memo expressions.
	physicalPropsMap map[*physicalProps]int32
	physicalProps    []*physicalProps
	// Private data attached to a memoExpr (indexed by memoExpr.private). Most
	// memo expressions do not contain private data allowing a modest savings of
	// 12 bytes per memoExpr.
	private []interface{}
	// The root group in the memo. This is the group for the expression added by
	// addRoot (i.e. the expression that we're optimizing).
	root groupID
}

func newMemo() *memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression. Similarly,
	// index 0 for the private data is reserved.
	return &memo{
		exprMap:          make(map[memoExprFingerprint]groupID),
		groupMap:         make(map[memoExprFingerprint]groupID),
		groups:           make([]memoGroup, 1),
		physicalPropsMap: make(map[*physicalProps]int32),
		physicalProps:    make([]*physicalProps, 1),
		private:          make([]interface{}, 1),
	}
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for _, id := range m.topologicalSort() {
		g := &m.groups[id]
		fmt.Fprintf(&buf, "%d:", id)
		for _, e := range g.exprs {
			fmt.Fprintf(&buf, " %s", e.DebugString(m))
		}
		if false {
			for f, opt := range g.optMap {
				fmt.Fprintf(&buf, " {%s:%d:%0.1f}", f, opt.loc.expr, opt.cost)
			}
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

// topologicalSort returns an ordering of memo groups such that if an expression
// in group i points to group j, i comes before j in the ordering.
func (m *memo) topologicalSort() []groupID {
	visited := make([]bool, len(m.groups))
	res := make([]groupID, 0, len(m.groups))
	for id := range m.groups[1:] {
		res = m.dfsVisit(groupID(id+1), visited, res)
	}

	// The depth first search returned the groups from leaf to root. We want the
	// root first, so reverse the results.
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

// dfsVisit performs a depth-first search starting from the group, avoiding
// already visited nodes. Returns the visited node in depth-first order.
func (m *memo) dfsVisit(id groupID, visited []bool, res []groupID) []groupID {
	if id <= 0 || visited[id] {
		return res
	}
	visited[id] = true

	g := &m.groups[id]
	for _, e := range g.exprs {
		for _, v := range e.children(m) {
			res = m.dfsVisit(v, visited, res)
		}
	}
	return append(res, id)
}

func (m *memo) addRoot(e *expr) {
	if m.root != 0 {
		fatalf("root has already been set")
	}
	m.root = m.addExpr(e)
}

// addExpr adds an expression to the memo and returns the group it was added to.
func (m *memo) addExpr(e *expr) groupID {
	if e.loc.group > 0 && e.loc.expr >= 0 {
		// The expression has already been added to the memo.
		return e.loc.group
	}

	// Build a memoExpr and check to see if it already exists in the memo.
	me := memoExpr{
		op: e.op,
	}
	if len(e.children) <= numInlineChildren {
		me.numChildren = int32(len(e.children))
	} else {
		idx := int32(len(m.children))
		me.numChildren = numInlineChildren + idx + 1
		m.children = append(m.children, make([]groupID, len(e.children)))
	}

	children := me.children(m)
	for i, g := range e.children {
		if g != nil {
			children[i] = m.addExpr(g)
		}
	}

	if e.physicalProps != nil {
		i, ok := m.physicalPropsMap[e.physicalProps]
		if !ok {
			i = int32(len(m.physicalProps))
			m.physicalPropsMap[e.physicalProps] = i
			m.physicalProps = append(m.physicalProps, e.physicalProps)
		}
		me.physicalProps = i
	}

	if e.private != nil {
		me.private = int32(len(m.private))
		m.private = append(m.private, e.private)
	}

	// Normalize the child order for operators which are not sensitive to the input
	// order.
	//
	// TODO(peter): this should likely be a method on the operator.
	switch me.op {
	case listOp, andOp, orOp:
		children := me.children(m)
		sort.Slice(children, func(i, j int) bool {
			return children[i] < children[j]
		})
	}

	ef := me.fingerprint(m)
	if group, ok := m.exprMap[ef]; ok {
		// The expression already exists in the memo.
		if me.numChildren > numInlineChildren {
			if me.numChildren-numInlineChildren == int32(len(m.children)) {
				// Remove the child slice we added. This is a space optimization and
				// not strictly necessary.
				m.children = m.children[:len(m.children)-1]
			}
		}
		if me.private == int32(len(m.private))-1 {
			// Remove the private data we added.
			m.private = m.private[:me.private]
		}
		return group
	}

	group := e.loc.group
	if group == 0 {
		// Determine which group the expression belongs in, creating it if
		// necessary.
		var ok bool
		gf := me.groupFingerprint(m)
		group, ok = m.groupMap[gf]
		if !ok {
			group = groupID(len(m.groups))
			m.groups = append(m.groups, memoGroup{
				id:          group,
				exprMap:     make(map[memoExprFingerprint]exprID, 1),
				props:       e.props,
				scalarProps: e.scalarProps,
			})
			m.groupMap[gf] = group
		}
	}

	g := &m.groups[group]
	if _, ok := g.exprMap[ef]; !ok {
		g.exprMap[ef] = exprID(len(g.exprs))
		g.exprs = append(g.exprs, me)
	}
	m.exprMap[ef] = group
	return group
}

// Bind creates a cursor expression rooted at the specified location that
// matches the pattern. The cursor can be advanced with calls to advance().
//
// Returns nil if the pattern does not match any expression rooted at the
// specified location.
func (m *memo) bind(loc memoLoc, pattern *expr) *expr {
	g := &m.groups[loc.group]
	e := &g.exprs[loc.expr]
	if !e.matchOp(pattern) {
		return nil
	}

	cursor := &expr{
		op:            e.op,
		loc:           loc,
		children:      make([]*expr, len(e.children(m))),
		props:         g.props,
		scalarProps:   g.scalarProps,
		physicalProps: m.physicalProps[e.physicalProps],
		private:       m.private[e.private],
	}

	if isPatternLeaf(pattern) {
		return cursor
	}

	// Initialize the child cursors.
	for i, g := range e.children(m) {
		childPattern := childPattern(pattern, i)
		if g == 0 {
			// No child present.
			if !isPatternSentinel(childPattern) {
				return nil
			}
			// Leave the nil cursor, it will be skipped by advance.
			continue
		}

		cursor.children[i] = m.bindGroup(&m.groups[g], childPattern)
		if cursor.children[i] == nil {
			// Pattern failed to match.
			return nil
		}
	}
	return cursor
}

// advance returns the next cursor expression that matches the pattern.
// The cursor must have been obtained from a previous call to bind() or
// advance().
//
// Returns nil if there are no more expressions that match.
func (m *memo) advance(loc memoLoc, pattern, cursor *expr) *expr {
	e := &m.groups[loc.group].exprs[loc.expr]
	if loc != cursor.loc || !e.matchOp(pattern) {
		fatalf("cursor mismatch: e: %s %s  cursor: %s %s", e.op, loc, cursor.op, cursor.loc)
	}

	if isPatternLeaf(pattern) {
		// For a leaf pattern we have only the initial binding.
		return nil
	}

	// We first advance the first child cursor; when that is exhausted, we reset
	// it and advance the second cursor. Next time we will start over with
	// advancing the first child cursor until it is exhausted.
	//
	// For example, say we have three children with 2 bindings each:
	//            child 0  child 1  child 2
	// bind:      0        0        0
	// advance:   1        0        0
	// advance:   0        1        0
	// advance:   1        1        0
	// advance:   0        0        1
	// advance:   1        0        1
	// advance:   0        1        1
	// advance:   1        1        1
	// advance:   done
	//
	// This is somewhat analogous to incrementing an integer (children are digits,
	// in reverse order).
	for i, childCursor := range cursor.children {
		if childCursor == nil {
			// Skip the missing child (it must be a pattern leaf).
			continue
		}

		childPattern := childPattern(pattern, i)

		g := &m.groups[childCursor.loc.group]
		cursor.children[i] = m.advanceGroup(g, childPattern, childCursor)
		if cursor.children[i] != nil {
			// We successfully advanced a child.
			return cursor
		}

		// Reset the child cursor and advance to the next child.
		cursor.children[i] = m.bindGroup(g, childPattern)
	}
	// We exhausted all child cursors. Nothing more for us to do.
	return nil
}

// bindGroup is similar to bind, except that it can bind any expression
// rooted in the given group.
//
// Returns a cursor expression that can be advanced with advanceGroup().
//
// Returns nil if the pattern does not match any expression rooted at the
// specified location.
func (m *memo) bindGroup(g *memoGroup, pattern *expr) *expr {
	for i, e := range g.exprs {
		if !e.matchOp(pattern) {
			continue
		}
		if cursor := m.bind(memoLoc{g.id, exprID(i)}, pattern); cursor != nil {
			return cursor
		}
	}

	// The group has no expressions that match the pattern.
	return nil
}

// advanceGroup advances a cursor expression obtained from a previous call to
// bindGroup() or advanceGroup().
//
// Returns nil if there are no more expressions in the group that match the
// pattern.
func (m *memo) advanceGroup(g *memoGroup, pattern, cursor *expr) *expr {
	if isPatternLeaf(pattern) {
		// For leaf patterns we do not iterate on groups.
		return nil
	}

	// Try to advance the binding for the current expression.
	if c := m.advance(cursor.loc, pattern, cursor); c != nil {
		return c
	}

	// Find another expression to bind.
	for i, e := range g.exprs[(cursor.loc.expr + 1):] {
		if !e.matchOp(pattern) {
			continue
		}
		loc := memoLoc{g.id, exprID(i) + cursor.loc.expr + 1}
		if c := m.bind(loc, pattern); c != nil {
			return c
		}
	}

	// We've exhausted the group.
	return nil
}

// extract recursively extracts the lowest cost expression that provides the
// specified properties from the specified group.
func (m *memo) extract(required *physicalProps, group groupID) *expr {
	opt, ok := m.groups[group].optMap[required.fingerprint()]
	if !ok {
		return nil
	}
	return m.extractBest(opt)
}

func (m *memo) extractBest(opt *memoOptState) *expr {
	g := &m.groups[opt.loc.group]
	e := &g.exprs[opt.loc.expr]
	r := &expr{
		op:            e.op,
		loc:           opt.loc,
		children:      make([]*expr, len(opt.children)),
		props:         g.props,
		scalarProps:   g.scalarProps,
		physicalProps: m.physicalProps[e.physicalProps],
		private:       m.private[e.private],
	}
	for i, c := range opt.children {
		r.children[i] = m.extractBest(c)
	}
	return r
}
