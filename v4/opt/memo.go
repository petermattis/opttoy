package opt

import (
	"crypto/md5"

	"github.com/petermattis/opttoy/v4/cat"
)

// PrivateID identifies custom private data used by a memo expression and
// stored by the memo. Privates have numbers greater than 0; a PrivateID of 0
// indicates an unknown private.
type PrivateID uint32

// ListID identifies a variable-sized list used by a memo expression and stored
// by the memo. The ID consists of an offset into the memo's lists slice, plus
// the number of elements in the list. Lists have numbers greater than 0; a
// ListID of 0 indicates an unknown list.
type ListID struct {
	offset uint32
	len    uint32
}

var UndefinedList ListID = ListID{}

// fingerprint contains the fingerprint of memoExpr. Two memo expressions are
// considered equal if their fingerprints are equal. The fast-path case for
// expressions that are 16 bytes are less is to copy the memo data directly
// into the fingerprint. In the slow-path case, the md5 hash of the memo data
// is computed and stored in the fingerprint.
type fingerprint [md5.Size]byte

// exprOffset contains the byte offset of a memoExpr in the memo's arena.
type exprOffset uint32

// exprLoc describes the group to which an expression belongs, as well as its
// own offset into the memo's arena.
type exprLoc struct {
	group  GroupID
	offset exprOffset
}

type memo struct {
	// catalog is the interface to database metadata and statistics.
	metadata *Metadata

	// In order to reduce memory usage and GC load, all memoExprs associated
	// with this memo are allocated from this arena. Expression references are
	// byte offsets into the arena (exprOffset).
	arena *arena

	// A map from expression fingerprint (memoExpr.fingerprint()) to that
	// expression's group and its offset into the arena. Note that the
	// exprOffset field may be zero for alternate fingerprints (i.e.
	// fingerprints for denormalized expressions).
	exprMap map[fingerprint]exprLoc

	// The slice of groups, indexed by group ID Note the group ID 0 is invalid
	// in order to allow zero initialization of an expression to indicate that
	// it did not originate from the memo.
	groups []memoGroup

	// logPropsFactory is used to derive logical properties for an expression,
	// based on its children.
	logPropsFactory logicalPropsFactory

	// Intern the set of unique physical properties used by expressions in the
	// memo, since there are so many duplicates.
	physPropsMap     map[string]physicalPropsID
	physProps        []PhysicalProps
	physPropsFactory physicalPropsFactory

	// Some memoExprs have a variable number of children. The memoExpr stores
	// the list as a ListID struct, which contains an index into this array,
	// plus the count of children. The children are stored as a slice of this
	// array. Note that ListID 0 is invalid in order to indicate an unknown
	// list.
	lists []GroupID

	// Optional private data attached to a memoExpr. It is stored here because
	// the arena cannot contain pointers. Note that PrivateID 0 is invalid in
	// order to indicate an unknown private.
	privates []interface{}

	// Intern the set of unique privates used by expressions in the memo, since
	// there are so many duplicates.
	privatesMap map[interface{}]PrivateID
}

func newMemo(catalog *cat.Catalog) *memo {
	// NB: group 0 is reserved and intentionally nil so that the 0 group index
	// can indicate that we don't know the group for an expression. Similarly,
	// index 0 for private data, index 0 for physical properties, and index 0
	// for lists are all reserved.
	m := &memo{
		metadata:     newMetadata(catalog),
		arena:        newArena(),
		exprMap:      make(map[fingerprint]exprLoc),
		groups:       make([]memoGroup, 1),
		physPropsMap: make(map[string]physicalPropsID),
		physProps:    make([]PhysicalProps, 1, 2),
		lists:        make([]GroupID, 1),
		privates:     make([]interface{}, 1),
		privatesMap:  make(map[interface{}]PrivateID),
	}

	m.logPropsFactory.init(m)
	m.physPropsFactory.init(m)

	// Intern default physical properties.
	physProps := PhysicalProps{}
	m.physProps = append(m.physProps, physProps)
	m.physPropsMap[physProps.fingerprint()] = defaultPhysPropsID

	return m
}

func (m *memo) newGroup(op Operator, offset exprOffset) *memoGroup {
	id := GroupID(len(m.groups))
	exprs := make([]exprOffset, 0, 1)
	bestExprsMap := make(map[physicalPropsID]int)
	m.groups = append(m.groups, memoGroup{id: id, norm: offset, exprs: exprs, bestExprsMap: bestExprsMap})
	return &m.groups[len(m.groups)-1]
}

// addFingerprint checks whether the given fingerprint already references the
// given group. If not, it creates a new reference to that group, but without
// the offset of the corresponding expression. This is used when the optimizer
// creates expressions that are not normalized, but are not intended to be part
// of the search space. In that case, there's no reason to occupy space in the
// arena. However, it's still useful to store the fingerprint in order to avoid
// re-normalizing that expression in the future.
func (m *memo) addAltFingerprint(alt fingerprint, group GroupID) {
	existing, ok := m.exprMap[alt]
	if ok {
		if existing.group != group {
			panic("same fingerprint cannot map to different groups")
		}
	} else {
		m.exprMap[alt] = exprLoc{group: group}
	}
}

func (m *memo) lookupGroup(group GroupID) *memoGroup {
	return &m.groups[group]
}

func (m *memo) lookupGroupByFingerprint(f fingerprint) GroupID {
	return m.exprMap[f].group
}

// lookupNormExpr returns the normal form of all logically equivalent
// expressions in the group.
func (m *memo) lookupNormExpr(group GroupID) *memoExpr {
	return m.lookupExpr(m.groups[group].norm)
}

func (m *memo) lookupExpr(offset exprOffset) *memoExpr {
	return (*memoExpr)(m.arena.getPointer(uint32(offset)))
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

// TODO: Add string representations.
