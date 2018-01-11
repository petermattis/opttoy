package opt

import (
	"bytes"
	"fmt"

	"github.com/petermattis/opttoy/v4/cat"
)

// PrivateID identifies custom private data used by a memo expression and
// stored by the memo. Privates have numbers greater than 0; a PrivateID of 0
// indicates an unknown private.
type PrivateID uint32

// ListID identifies a variable-sized list used by a memo expression and stored
// by the memo. The ID consists of an offset into the memo's lists slice, plus
// the number of elements in the list. Lists have numbers greater than 0; a
// ListID of 0 indicates an undefined list (possible indicator of a bug).
type ListID struct {
	offset uint32
	len    uint32
}

// undefinedList is reserved to indicate an uninitialized list identifier, in
// order to catch bugs.
var undefinedList ListID = ListID{}

// isEmpty is true if the list contains no elements.
func (l ListID) isEmpty() bool {
	return l.len == 0
}

// memoLoc describes the location of an expression in the memo, which is a
// tuple of the expression's memo group and its index within that group.
type memoLoc struct {
	group GroupID
	expr  exprID
}

type memo struct {
	// metadata provides access to database metadata and statistics, as well as
	// information about the columns and tables used in this particular query.
	metadata *Metadata

	// exprMap maps from expression fingerprint (memoExpr.fingerprint()) to
	// that expression's group.
	exprMap map[fingerprint]GroupID

	// groups is the set of all groups in the memo, indexed by group ID. Note
	// the group ID 0 is invalid in order to allow zero initialization of an
	// expression to indicate that it did not originate from the memo.
	groups []memoGroup

	// logPropsFactory is used to derive logical properties for an expression,
	// based on the logical properties of its children.
	logPropsFactory logicalPropsFactory

	// physPropsFactory is used to derive required physical properties for the
	// children an expression, based on the required physical properties for
	// the parent.
	physPropsFactory physicalPropsFactory

	// Intern the set of unique physical properties used by expressions in the
	// memo, since there are so many duplicates.
	physPropsMap map[string]physicalPropsID
	physProps    []PhysicalProps

	// Some memoExprs have a variable number of children. The memoExpr stores
	// the list as a ListID struct, which contains an index into this array,
	// plus the count of children. The children are stored as a slice of this
	// array. Note that ListID 0 is invalid in order to indicate an unknown
	// list.
	lists []GroupID

	// Intern the set of unique privates used by expressions in the memo, since
	// there are so many duplicates. Note that PrivateID 0 is invalid in order
	// to indicate an unknown private.
	privatesMap map[interface{}]PrivateID
	privates    []interface{}
}

func newMemo(catalog *cat.Catalog) *memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression. Similarly,
	// index 0 for private data, index 0 for physical properties, and index 0
	// for lists are all reserved.
	m := &memo{
		metadata:     newMetadata(catalog),
		exprMap:      make(map[fingerprint]GroupID),
		groups:       make([]memoGroup, 1),
		physPropsMap: make(map[string]physicalPropsID),
		physProps:    make([]PhysicalProps, 1, 2),
		lists:        make([]GroupID, 1),
		privatesMap:  make(map[interface{}]PrivateID),
		privates:     make([]interface{}, 1),
	}

	m.logPropsFactory.init(m)
	m.physPropsFactory.init(m)

	// Intern default physical properties.
	physProps := PhysicalProps{}
	m.physProps = append(m.physProps, physProps)
	m.physPropsMap[physProps.fingerprint()] = defaultPhysPropsID

	return m
}

// newGroup creates a new group and adds it to the memo.
func (m *memo) newGroup(norm *memoExpr) *memoGroup {
	id := GroupID(len(m.groups))
	exprs := []memoExpr{*norm}
	m.groups = append(m.groups, memoGroup{
		id:           id,
		exprs:        exprs,
		bestExprsMap: make(map[physicalPropsID]int),
	})
	return &m.groups[len(m.groups)-1]
}

// addAltFingerprint checks whether the given fingerprint already references
// the given group. If not, it creates a new reference to that group, but
// without the offset of the corresponding expression. This is used when the
// optimizer creates expressions that are not normalized, but are not intended
// to be part of the search space. In that case, there's no reason to occupy
// space in the group's exprs array. However, it's still useful to store the
// fingerprint in order to avoid re-normalizing that expression in the future.
func (m *memo) addAltFingerprint(alt fingerprint, group GroupID) {
	existing, ok := m.exprMap[alt]
	if ok {
		if existing != group {
			panic("same fingerprint cannot map to different groups")
		}
	} else {
		m.exprMap[alt] = group
	}
}

// memoizeNormExpr enters a normalized expression into the memo. This requires
// the creation of a new memo group with the normalized expression as its first
// expression.
func (m *memo) memoizeNormExpr(norm *memoExpr) GroupID {
	if m.exprMap[norm.fingerprint()] != 0 {
		panic("normalized expression has been entered into the memo more than once")
	}

	mgrp := m.newGroup(norm)
	e := makeExpr(m, mgrp.id, defaultPhysPropsID)
	mgrp.logical = m.logPropsFactory.constructProps(&e)

	m.exprMap[norm.fingerprint()] = mgrp.id
	return mgrp.id
}

// memoizeDenormExpr enters a denormalized expression into the given memo
// group. The group must have already been created, since the normalized
// version of the expression should have triggered its creation earlier.
func (m *memo) memoizeDenormExpr(group GroupID, denorm *memoExpr) {
	existing := m.exprMap[denorm.fingerprint()]
	if existing != 0 {
		// Expression has already been entered into the memo.
		if existing != group {
			panic("denormalized expression's group doesn't match fingerprint group")
		}
	} else {
		// Add the denormalized expression to the memo.
		m.lookupGroup(group).addExpr(denorm)
		m.exprMap[denorm.fingerprint()] = group
	}
}

func (m *memo) lookupGroup(group GroupID) *memoGroup {
	return &m.groups[group]
}

func (m *memo) lookupGroupByFingerprint(f fingerprint) GroupID {
	return m.exprMap[f]
}

func (m *memo) lookupExpr(loc memoLoc) *memoExpr {
	return m.groups[loc.group].lookupExpr(loc.expr)
}

func (m *memo) lookupNormExpr(group GroupID) *memoExpr {
	return m.groups[group].lookupExpr(normExprID)
}

func (m *memo) storeList(items []GroupID) ListID {
	id := ListID{offset: uint32(len(m.lists)), len: uint32(len(items))}
	m.lists = append(m.lists, items...)
	return id
}

func (m *memo) lookupList(id ListID) []GroupID {
	return m.lists[id.offset : id.offset+id.len]
}

func (m *memo) internPrivate(private interface{}) PrivateID {
	id, ok := m.privatesMap[private]
	if !ok {
		id = PrivateID(len(m.privates))
		m.privates = append(m.privates, private)
		m.privatesMap[private] = id
	}

	return id
}

func (m *memo) lookupPrivate(id PrivateID) interface{} {
	return m.privates[id]
}

func (m *memo) internPhysicalProps(props *PhysicalProps) physicalPropsID {
	// Intern the physical properties since there are likely to be many
	// duplicates.
	fingerprint := props.fingerprint()
	id, ok := m.physPropsMap[fingerprint]
	if !ok {
		id = physicalPropsID(len(m.physProps))
		m.physProps = append(m.physProps, *props)
		m.physPropsMap[fingerprint] = id
	}
	return id
}

func (m *memo) lookupPhysicalProps(id physicalPropsID) *PhysicalProps {
	return &m.physProps[id]
}

func (m *memo) String() string {
	var buf bytes.Buffer
	for i := len(m.groups) - 1; i > 0; i-- {
		mgrp := &m.groups[i]
		fmt.Fprintf(&buf, "%d:", i)
		for i := range mgrp.exprs {
			mexpr := &mgrp.exprs[i]
			fmt.Fprintf(&buf, " %s", mexpr.memoString(m, mgrp))
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}
