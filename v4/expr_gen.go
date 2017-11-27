package v4

import (
	"crypto/md5"
	"unsafe"
)

type expr interface {
	fingerprint() exprFingerprint
	operator() operator
	childCount(m *memo) int
	child(m *memo, n int) groupID
	private(m *memo) interface{}
	logicalProps(m *memo) *logicalProps

	isScalar() bool
	isLogical() bool
	isPhysical() bool
	isRelational() bool
}

type variableExpr struct {
	group groupID
	op    operator
	col   privateID
}

func (e *variableExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(variableExpr{})
	const offset = unsafe.Offsetof(variableExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *variableExpr) operator() operator {
	return variableOp
}

func (e *variableExpr) childCount(m *memo) int {
	return 1
}

func (e *variableExpr) child(m *memo, n int) groupID {
	switch n {
	default:
		panic("child index out of range")
	}
}

func (e *variableExpr) private(m *memo) interface{} {
	return nil
}

func (e *variableExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *variableExpr) isScalar() bool {
	return true
}

func (e *variableExpr) isLogical() bool {
	return true
}

func (e *variableExpr) isPhysical() bool {
	return true
}

func (e *variableExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asVariable() *variableExpr {
	if m.op != variableOp {
		return nil
	}

	return (*variableExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeVariable(e *variableExpr) groupID {
	const size = uint32(unsafe.Sizeof(variableExpr{}))
	const align = uint32(unsafe.Alignof(variableExpr{}))

	variableOffset := m.lookupExprByFingerprint(e.fingerprint())
	if variableOffset != 0 {
		return m.lookupExpr(variableOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*variableExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type constExpr struct {
	group groupID
	op    operator
	value privateID
}

func (e *constExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(constExpr{})
	const offset = unsafe.Offsetof(constExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *constExpr) operator() operator {
	return constOp
}

func (e *constExpr) childCount(m *memo) int {
	return 1
}

func (e *constExpr) child(m *memo, n int) groupID {
	switch n {
	default:
		panic("child index out of range")
	}
}

func (e *constExpr) private(m *memo) interface{} {
	return nil
}

func (e *constExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *constExpr) isScalar() bool {
	return true
}

func (e *constExpr) isLogical() bool {
	return true
}

func (e *constExpr) isPhysical() bool {
	return true
}

func (e *constExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asConst() *constExpr {
	if m.op != constOp {
		return nil
	}

	return (*constExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeConst(e *constExpr) groupID {
	const size = uint32(unsafe.Sizeof(constExpr{}))
	const align = uint32(unsafe.Alignof(constExpr{}))

	constOffset := m.lookupExprByFingerprint(e.fingerprint())
	if constOffset != 0 {
		return m.lookupExpr(constOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*constExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type listExpr struct {
	group groupID
	op    operator
	items listID
}

func (e *listExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(listExpr{})
	const offset = unsafe.Offsetof(listExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *listExpr) operator() operator {
	return listOp
}

func (e *listExpr) childCount(m *memo) int {
	return 0 + int(e.items.len)
}

func (e *listExpr) child(m *memo, n int) groupID {
	switch n {
	default:
		list := m.lookupList(e.items)
		return list[n-0]
	}
}

func (e *listExpr) private(m *memo) interface{} {
	return nil
}

func (e *listExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *listExpr) isScalar() bool {
	return true
}

func (e *listExpr) isLogical() bool {
	return true
}

func (e *listExpr) isPhysical() bool {
	return true
}

func (e *listExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asList() *listExpr {
	if m.op != listOp {
		return nil
	}

	return (*listExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeList(e *listExpr) groupID {
	const size = uint32(unsafe.Sizeof(listExpr{}))
	const align = uint32(unsafe.Alignof(listExpr{}))

	listOffset := m.lookupExprByFingerprint(e.fingerprint())
	if listOffset != 0 {
		return m.lookupExpr(listOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*listExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type orderedListExpr struct {
	group groupID
	op    operator
	items listID
}

func (e *orderedListExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(orderedListExpr{})
	const offset = unsafe.Offsetof(orderedListExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *orderedListExpr) operator() operator {
	return orderedListOp
}

func (e *orderedListExpr) childCount(m *memo) int {
	return 0 + int(e.items.len)
}

func (e *orderedListExpr) child(m *memo, n int) groupID {
	switch n {
	default:
		list := m.lookupList(e.items)
		return list[n-0]
	}
}

func (e *orderedListExpr) private(m *memo) interface{} {
	return nil
}

func (e *orderedListExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *orderedListExpr) isScalar() bool {
	return true
}

func (e *orderedListExpr) isLogical() bool {
	return true
}

func (e *orderedListExpr) isPhysical() bool {
	return true
}

func (e *orderedListExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asOrderedList() *orderedListExpr {
	if m.op != orderedListOp {
		return nil
	}

	return (*orderedListExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeOrderedList(e *orderedListExpr) groupID {
	const size = uint32(unsafe.Sizeof(orderedListExpr{}))
	const align = uint32(unsafe.Alignof(orderedListExpr{}))

	orderedListOffset := m.lookupExprByFingerprint(e.fingerprint())
	if orderedListOffset != 0 {
		return m.lookupExpr(orderedListOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*orderedListExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type existsExpr struct {
	group groupID
	op    operator
	input groupID
}

func (e *existsExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(existsExpr{})
	const offset = unsafe.Offsetof(existsExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *existsExpr) operator() operator {
	return existsOp
}

func (e *existsExpr) childCount(m *memo) int {
	return 1
}

func (e *existsExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.input
	default:
		panic("child index out of range")
	}
}

func (e *existsExpr) private(m *memo) interface{} {
	return nil
}

func (e *existsExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *existsExpr) isScalar() bool {
	return true
}

func (e *existsExpr) isLogical() bool {
	return true
}

func (e *existsExpr) isPhysical() bool {
	return true
}

func (e *existsExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asExists() *existsExpr {
	if m.op != existsOp {
		return nil
	}

	return (*existsExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeExists(e *existsExpr) groupID {
	const size = uint32(unsafe.Sizeof(existsExpr{}))
	const align = uint32(unsafe.Alignof(existsExpr{}))

	existsOffset := m.lookupExprByFingerprint(e.fingerprint())
	if existsOffset != 0 {
		return m.lookupExpr(existsOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*existsExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type andExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *andExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(andExpr{})
	const offset = unsafe.Offsetof(andExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *andExpr) operator() operator {
	return andOp
}

func (e *andExpr) childCount(m *memo) int {
	return 2
}

func (e *andExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *andExpr) private(m *memo) interface{} {
	return nil
}

func (e *andExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *andExpr) isScalar() bool {
	return true
}

func (e *andExpr) isLogical() bool {
	return true
}

func (e *andExpr) isPhysical() bool {
	return true
}

func (e *andExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asAnd() *andExpr {
	if m.op != andOp {
		return nil
	}

	return (*andExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAnd(e *andExpr) groupID {
	const size = uint32(unsafe.Sizeof(andExpr{}))
	const align = uint32(unsafe.Alignof(andExpr{}))

	andOffset := m.lookupExprByFingerprint(e.fingerprint())
	if andOffset != 0 {
		return m.lookupExpr(andOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*andExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type orExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *orExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(orExpr{})
	const offset = unsafe.Offsetof(orExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *orExpr) operator() operator {
	return orOp
}

func (e *orExpr) childCount(m *memo) int {
	return 2
}

func (e *orExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *orExpr) private(m *memo) interface{} {
	return nil
}

func (e *orExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *orExpr) isScalar() bool {
	return true
}

func (e *orExpr) isLogical() bool {
	return true
}

func (e *orExpr) isPhysical() bool {
	return true
}

func (e *orExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asOr() *orExpr {
	if m.op != orOp {
		return nil
	}

	return (*orExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeOr(e *orExpr) groupID {
	const size = uint32(unsafe.Sizeof(orExpr{}))
	const align = uint32(unsafe.Alignof(orExpr{}))

	orOffset := m.lookupExprByFingerprint(e.fingerprint())
	if orOffset != 0 {
		return m.lookupExpr(orOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*orExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notExpr struct {
	group groupID
	op    operator
	input groupID
}

func (e *notExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notExpr{})
	const offset = unsafe.Offsetof(notExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notExpr) operator() operator {
	return notOp
}

func (e *notExpr) childCount(m *memo) int {
	return 1
}

func (e *notExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.input
	default:
		panic("child index out of range")
	}
}

func (e *notExpr) private(m *memo) interface{} {
	return nil
}

func (e *notExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notExpr) isScalar() bool {
	return true
}

func (e *notExpr) isLogical() bool {
	return true
}

func (e *notExpr) isPhysical() bool {
	return true
}

func (e *notExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNot() *notExpr {
	if m.op != notOp {
		return nil
	}

	return (*notExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNot(e *notExpr) groupID {
	const size = uint32(unsafe.Sizeof(notExpr{}))
	const align = uint32(unsafe.Alignof(notExpr{}))

	notOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notOffset != 0 {
		return m.lookupExpr(notOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type eqExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *eqExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(eqExpr{})
	const offset = unsafe.Offsetof(eqExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *eqExpr) operator() operator {
	return eqOp
}

func (e *eqExpr) childCount(m *memo) int {
	return 2
}

func (e *eqExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *eqExpr) private(m *memo) interface{} {
	return nil
}

func (e *eqExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *eqExpr) isScalar() bool {
	return true
}

func (e *eqExpr) isLogical() bool {
	return true
}

func (e *eqExpr) isPhysical() bool {
	return true
}

func (e *eqExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asEq() *eqExpr {
	if m.op != eqOp {
		return nil
	}

	return (*eqExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeEq(e *eqExpr) groupID {
	const size = uint32(unsafe.Sizeof(eqExpr{}))
	const align = uint32(unsafe.Alignof(eqExpr{}))

	eqOffset := m.lookupExprByFingerprint(e.fingerprint())
	if eqOffset != 0 {
		return m.lookupExpr(eqOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*eqExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type ltExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *ltExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(ltExpr{})
	const offset = unsafe.Offsetof(ltExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *ltExpr) operator() operator {
	return ltOp
}

func (e *ltExpr) childCount(m *memo) int {
	return 2
}

func (e *ltExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *ltExpr) private(m *memo) interface{} {
	return nil
}

func (e *ltExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *ltExpr) isScalar() bool {
	return true
}

func (e *ltExpr) isLogical() bool {
	return true
}

func (e *ltExpr) isPhysical() bool {
	return true
}

func (e *ltExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asLt() *ltExpr {
	if m.op != ltOp {
		return nil
	}

	return (*ltExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLt(e *ltExpr) groupID {
	const size = uint32(unsafe.Sizeof(ltExpr{}))
	const align = uint32(unsafe.Alignof(ltExpr{}))

	ltOffset := m.lookupExprByFingerprint(e.fingerprint())
	if ltOffset != 0 {
		return m.lookupExpr(ltOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*ltExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type gtExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *gtExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(gtExpr{})
	const offset = unsafe.Offsetof(gtExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *gtExpr) operator() operator {
	return gtOp
}

func (e *gtExpr) childCount(m *memo) int {
	return 2
}

func (e *gtExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *gtExpr) private(m *memo) interface{} {
	return nil
}

func (e *gtExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *gtExpr) isScalar() bool {
	return true
}

func (e *gtExpr) isLogical() bool {
	return true
}

func (e *gtExpr) isPhysical() bool {
	return true
}

func (e *gtExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asGt() *gtExpr {
	if m.op != gtOp {
		return nil
	}

	return (*gtExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeGt(e *gtExpr) groupID {
	const size = uint32(unsafe.Sizeof(gtExpr{}))
	const align = uint32(unsafe.Alignof(gtExpr{}))

	gtOffset := m.lookupExprByFingerprint(e.fingerprint())
	if gtOffset != 0 {
		return m.lookupExpr(gtOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*gtExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type leExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *leExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(leExpr{})
	const offset = unsafe.Offsetof(leExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *leExpr) operator() operator {
	return leOp
}

func (e *leExpr) childCount(m *memo) int {
	return 2
}

func (e *leExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *leExpr) private(m *memo) interface{} {
	return nil
}

func (e *leExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *leExpr) isScalar() bool {
	return true
}

func (e *leExpr) isLogical() bool {
	return true
}

func (e *leExpr) isPhysical() bool {
	return true
}

func (e *leExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asLe() *leExpr {
	if m.op != leOp {
		return nil
	}

	return (*leExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLe(e *leExpr) groupID {
	const size = uint32(unsafe.Sizeof(leExpr{}))
	const align = uint32(unsafe.Alignof(leExpr{}))

	leOffset := m.lookupExprByFingerprint(e.fingerprint())
	if leOffset != 0 {
		return m.lookupExpr(leOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*leExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type geExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *geExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(geExpr{})
	const offset = unsafe.Offsetof(geExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *geExpr) operator() operator {
	return geOp
}

func (e *geExpr) childCount(m *memo) int {
	return 2
}

func (e *geExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *geExpr) private(m *memo) interface{} {
	return nil
}

func (e *geExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *geExpr) isScalar() bool {
	return true
}

func (e *geExpr) isLogical() bool {
	return true
}

func (e *geExpr) isPhysical() bool {
	return true
}

func (e *geExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asGe() *geExpr {
	if m.op != geOp {
		return nil
	}

	return (*geExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeGe(e *geExpr) groupID {
	const size = uint32(unsafe.Sizeof(geExpr{}))
	const align = uint32(unsafe.Alignof(geExpr{}))

	geOffset := m.lookupExprByFingerprint(e.fingerprint())
	if geOffset != 0 {
		return m.lookupExpr(geOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*geExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type neExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *neExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(neExpr{})
	const offset = unsafe.Offsetof(neExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *neExpr) operator() operator {
	return neOp
}

func (e *neExpr) childCount(m *memo) int {
	return 2
}

func (e *neExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *neExpr) private(m *memo) interface{} {
	return nil
}

func (e *neExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *neExpr) isScalar() bool {
	return true
}

func (e *neExpr) isLogical() bool {
	return true
}

func (e *neExpr) isPhysical() bool {
	return true
}

func (e *neExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNe() *neExpr {
	if m.op != neOp {
		return nil
	}

	return (*neExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNe(e *neExpr) groupID {
	const size = uint32(unsafe.Sizeof(neExpr{}))
	const align = uint32(unsafe.Alignof(neExpr{}))

	neOffset := m.lookupExprByFingerprint(e.fingerprint())
	if neOffset != 0 {
		return m.lookupExpr(neOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*neExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type inOpExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *inOpExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(inOpExpr{})
	const offset = unsafe.Offsetof(inOpExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *inOpExpr) operator() operator {
	return inOpOp
}

func (e *inOpExpr) childCount(m *memo) int {
	return 2
}

func (e *inOpExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *inOpExpr) private(m *memo) interface{} {
	return nil
}

func (e *inOpExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *inOpExpr) isScalar() bool {
	return true
}

func (e *inOpExpr) isLogical() bool {
	return true
}

func (e *inOpExpr) isPhysical() bool {
	return true
}

func (e *inOpExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asInOp() *inOpExpr {
	if m.op != inOpOp {
		return nil
	}

	return (*inOpExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeInOp(e *inOpExpr) groupID {
	const size = uint32(unsafe.Sizeof(inOpExpr{}))
	const align = uint32(unsafe.Alignof(inOpExpr{}))

	inOpOffset := m.lookupExprByFingerprint(e.fingerprint())
	if inOpOffset != 0 {
		return m.lookupExpr(inOpOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*inOpExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notInExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *notInExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notInExpr{})
	const offset = unsafe.Offsetof(notInExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notInExpr) operator() operator {
	return notInOp
}

func (e *notInExpr) childCount(m *memo) int {
	return 2
}

func (e *notInExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *notInExpr) private(m *memo) interface{} {
	return nil
}

func (e *notInExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notInExpr) isScalar() bool {
	return true
}

func (e *notInExpr) isLogical() bool {
	return true
}

func (e *notInExpr) isPhysical() bool {
	return true
}

func (e *notInExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNotIn() *notInExpr {
	if m.op != notInOp {
		return nil
	}

	return (*notInExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotIn(e *notInExpr) groupID {
	const size = uint32(unsafe.Sizeof(notInExpr{}))
	const align = uint32(unsafe.Alignof(notInExpr{}))

	notInOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notInOffset != 0 {
		return m.lookupExpr(notInOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notInExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type likeExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *likeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(likeExpr{})
	const offset = unsafe.Offsetof(likeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *likeExpr) operator() operator {
	return likeOp
}

func (e *likeExpr) childCount(m *memo) int {
	return 2
}

func (e *likeExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *likeExpr) private(m *memo) interface{} {
	return nil
}

func (e *likeExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *likeExpr) isScalar() bool {
	return true
}

func (e *likeExpr) isLogical() bool {
	return true
}

func (e *likeExpr) isPhysical() bool {
	return true
}

func (e *likeExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asLike() *likeExpr {
	if m.op != likeOp {
		return nil
	}

	return (*likeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLike(e *likeExpr) groupID {
	const size = uint32(unsafe.Sizeof(likeExpr{}))
	const align = uint32(unsafe.Alignof(likeExpr{}))

	likeOffset := m.lookupExprByFingerprint(e.fingerprint())
	if likeOffset != 0 {
		return m.lookupExpr(likeOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*likeExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notLikeExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *notLikeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notLikeExpr{})
	const offset = unsafe.Offsetof(notLikeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notLikeExpr) operator() operator {
	return notLikeOp
}

func (e *notLikeExpr) childCount(m *memo) int {
	return 2
}

func (e *notLikeExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *notLikeExpr) private(m *memo) interface{} {
	return nil
}

func (e *notLikeExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notLikeExpr) isScalar() bool {
	return true
}

func (e *notLikeExpr) isLogical() bool {
	return true
}

func (e *notLikeExpr) isPhysical() bool {
	return true
}

func (e *notLikeExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNotLike() *notLikeExpr {
	if m.op != notLikeOp {
		return nil
	}

	return (*notLikeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotLike(e *notLikeExpr) groupID {
	const size = uint32(unsafe.Sizeof(notLikeExpr{}))
	const align = uint32(unsafe.Alignof(notLikeExpr{}))

	notLikeOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notLikeOffset != 0 {
		return m.lookupExpr(notLikeOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notLikeExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type iLikeExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *iLikeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(iLikeExpr{})
	const offset = unsafe.Offsetof(iLikeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *iLikeExpr) operator() operator {
	return iLikeOp
}

func (e *iLikeExpr) childCount(m *memo) int {
	return 2
}

func (e *iLikeExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *iLikeExpr) private(m *memo) interface{} {
	return nil
}

func (e *iLikeExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *iLikeExpr) isScalar() bool {
	return true
}

func (e *iLikeExpr) isLogical() bool {
	return true
}

func (e *iLikeExpr) isPhysical() bool {
	return true
}

func (e *iLikeExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asILike() *iLikeExpr {
	if m.op != iLikeOp {
		return nil
	}

	return (*iLikeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeILike(e *iLikeExpr) groupID {
	const size = uint32(unsafe.Sizeof(iLikeExpr{}))
	const align = uint32(unsafe.Alignof(iLikeExpr{}))

	iLikeOffset := m.lookupExprByFingerprint(e.fingerprint())
	if iLikeOffset != 0 {
		return m.lookupExpr(iLikeOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*iLikeExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notILikeExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *notILikeExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notILikeExpr{})
	const offset = unsafe.Offsetof(notILikeExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notILikeExpr) operator() operator {
	return notILikeOp
}

func (e *notILikeExpr) childCount(m *memo) int {
	return 2
}

func (e *notILikeExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *notILikeExpr) private(m *memo) interface{} {
	return nil
}

func (e *notILikeExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notILikeExpr) isScalar() bool {
	return true
}

func (e *notILikeExpr) isLogical() bool {
	return true
}

func (e *notILikeExpr) isPhysical() bool {
	return true
}

func (e *notILikeExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNotILike() *notILikeExpr {
	if m.op != notILikeOp {
		return nil
	}

	return (*notILikeExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotILike(e *notILikeExpr) groupID {
	const size = uint32(unsafe.Sizeof(notILikeExpr{}))
	const align = uint32(unsafe.Alignof(notILikeExpr{}))

	notILikeOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notILikeOffset != 0 {
		return m.lookupExpr(notILikeOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notILikeExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type similarToExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *similarToExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(similarToExpr{})
	const offset = unsafe.Offsetof(similarToExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *similarToExpr) operator() operator {
	return similarToOp
}

func (e *similarToExpr) childCount(m *memo) int {
	return 2
}

func (e *similarToExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *similarToExpr) private(m *memo) interface{} {
	return nil
}

func (e *similarToExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *similarToExpr) isScalar() bool {
	return true
}

func (e *similarToExpr) isLogical() bool {
	return true
}

func (e *similarToExpr) isPhysical() bool {
	return true
}

func (e *similarToExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asSimilarTo() *similarToExpr {
	if m.op != similarToOp {
		return nil
	}

	return (*similarToExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSimilarTo(e *similarToExpr) groupID {
	const size = uint32(unsafe.Sizeof(similarToExpr{}))
	const align = uint32(unsafe.Alignof(similarToExpr{}))

	similarToOffset := m.lookupExprByFingerprint(e.fingerprint())
	if similarToOffset != 0 {
		return m.lookupExpr(similarToOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*similarToExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notSimilarToExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *notSimilarToExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notSimilarToExpr{})
	const offset = unsafe.Offsetof(notSimilarToExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notSimilarToExpr) operator() operator {
	return notSimilarToOp
}

func (e *notSimilarToExpr) childCount(m *memo) int {
	return 2
}

func (e *notSimilarToExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *notSimilarToExpr) private(m *memo) interface{} {
	return nil
}

func (e *notSimilarToExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notSimilarToExpr) isScalar() bool {
	return true
}

func (e *notSimilarToExpr) isLogical() bool {
	return true
}

func (e *notSimilarToExpr) isPhysical() bool {
	return true
}

func (e *notSimilarToExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNotSimilarTo() *notSimilarToExpr {
	if m.op != notSimilarToOp {
		return nil
	}

	return (*notSimilarToExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotSimilarTo(e *notSimilarToExpr) groupID {
	const size = uint32(unsafe.Sizeof(notSimilarToExpr{}))
	const align = uint32(unsafe.Alignof(notSimilarToExpr{}))

	notSimilarToOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notSimilarToOffset != 0 {
		return m.lookupExpr(notSimilarToOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notSimilarToExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type regMatchExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *regMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(regMatchExpr{})
	const offset = unsafe.Offsetof(regMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *regMatchExpr) operator() operator {
	return regMatchOp
}

func (e *regMatchExpr) childCount(m *memo) int {
	return 2
}

func (e *regMatchExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *regMatchExpr) private(m *memo) interface{} {
	return nil
}

func (e *regMatchExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *regMatchExpr) isScalar() bool {
	return true
}

func (e *regMatchExpr) isLogical() bool {
	return true
}

func (e *regMatchExpr) isPhysical() bool {
	return true
}

func (e *regMatchExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asRegMatch() *regMatchExpr {
	if m.op != regMatchOp {
		return nil
	}

	return (*regMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRegMatch(e *regMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(regMatchExpr{}))
	const align = uint32(unsafe.Alignof(regMatchExpr{}))

	regMatchOffset := m.lookupExprByFingerprint(e.fingerprint())
	if regMatchOffset != 0 {
		return m.lookupExpr(regMatchOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*regMatchExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notRegMatchExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *notRegMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notRegMatchExpr{})
	const offset = unsafe.Offsetof(notRegMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notRegMatchExpr) operator() operator {
	return notRegMatchOp
}

func (e *notRegMatchExpr) childCount(m *memo) int {
	return 2
}

func (e *notRegMatchExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *notRegMatchExpr) private(m *memo) interface{} {
	return nil
}

func (e *notRegMatchExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notRegMatchExpr) isScalar() bool {
	return true
}

func (e *notRegMatchExpr) isLogical() bool {
	return true
}

func (e *notRegMatchExpr) isPhysical() bool {
	return true
}

func (e *notRegMatchExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNotRegMatch() *notRegMatchExpr {
	if m.op != notRegMatchOp {
		return nil
	}

	return (*notRegMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotRegMatch(e *notRegMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(notRegMatchExpr{}))
	const align = uint32(unsafe.Alignof(notRegMatchExpr{}))

	notRegMatchOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notRegMatchOffset != 0 {
		return m.lookupExpr(notRegMatchOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notRegMatchExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type regIMatchExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *regIMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(regIMatchExpr{})
	const offset = unsafe.Offsetof(regIMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *regIMatchExpr) operator() operator {
	return regIMatchOp
}

func (e *regIMatchExpr) childCount(m *memo) int {
	return 2
}

func (e *regIMatchExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *regIMatchExpr) private(m *memo) interface{} {
	return nil
}

func (e *regIMatchExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *regIMatchExpr) isScalar() bool {
	return true
}

func (e *regIMatchExpr) isLogical() bool {
	return true
}

func (e *regIMatchExpr) isPhysical() bool {
	return true
}

func (e *regIMatchExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asRegIMatch() *regIMatchExpr {
	if m.op != regIMatchOp {
		return nil
	}

	return (*regIMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRegIMatch(e *regIMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(regIMatchExpr{}))
	const align = uint32(unsafe.Alignof(regIMatchExpr{}))

	regIMatchOffset := m.lookupExprByFingerprint(e.fingerprint())
	if regIMatchOffset != 0 {
		return m.lookupExpr(regIMatchOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*regIMatchExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type notRegIMatchExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *notRegIMatchExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(notRegIMatchExpr{})
	const offset = unsafe.Offsetof(notRegIMatchExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *notRegIMatchExpr) operator() operator {
	return notRegIMatchOp
}

func (e *notRegIMatchExpr) childCount(m *memo) int {
	return 2
}

func (e *notRegIMatchExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *notRegIMatchExpr) private(m *memo) interface{} {
	return nil
}

func (e *notRegIMatchExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *notRegIMatchExpr) isScalar() bool {
	return true
}

func (e *notRegIMatchExpr) isLogical() bool {
	return true
}

func (e *notRegIMatchExpr) isPhysical() bool {
	return true
}

func (e *notRegIMatchExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asNotRegIMatch() *notRegIMatchExpr {
	if m.op != notRegIMatchOp {
		return nil
	}

	return (*notRegIMatchExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeNotRegIMatch(e *notRegIMatchExpr) groupID {
	const size = uint32(unsafe.Sizeof(notRegIMatchExpr{}))
	const align = uint32(unsafe.Alignof(notRegIMatchExpr{}))

	notRegIMatchOffset := m.lookupExprByFingerprint(e.fingerprint())
	if notRegIMatchOffset != 0 {
		return m.lookupExpr(notRegIMatchOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*notRegIMatchExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type isDistinctFromExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *isDistinctFromExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isDistinctFromExpr{})
	const offset = unsafe.Offsetof(isDistinctFromExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *isDistinctFromExpr) operator() operator {
	return isDistinctFromOp
}

func (e *isDistinctFromExpr) childCount(m *memo) int {
	return 2
}

func (e *isDistinctFromExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *isDistinctFromExpr) private(m *memo) interface{} {
	return nil
}

func (e *isDistinctFromExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *isDistinctFromExpr) isScalar() bool {
	return true
}

func (e *isDistinctFromExpr) isLogical() bool {
	return true
}

func (e *isDistinctFromExpr) isPhysical() bool {
	return true
}

func (e *isDistinctFromExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asIsDistinctFrom() *isDistinctFromExpr {
	if m.op != isDistinctFromOp {
		return nil
	}

	return (*isDistinctFromExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIsDistinctFrom(e *isDistinctFromExpr) groupID {
	const size = uint32(unsafe.Sizeof(isDistinctFromExpr{}))
	const align = uint32(unsafe.Alignof(isDistinctFromExpr{}))

	isDistinctFromOffset := m.lookupExprByFingerprint(e.fingerprint())
	if isDistinctFromOffset != 0 {
		return m.lookupExpr(isDistinctFromOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*isDistinctFromExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type isNotDistinctFromExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *isNotDistinctFromExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isNotDistinctFromExpr{})
	const offset = unsafe.Offsetof(isNotDistinctFromExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *isNotDistinctFromExpr) operator() operator {
	return isNotDistinctFromOp
}

func (e *isNotDistinctFromExpr) childCount(m *memo) int {
	return 2
}

func (e *isNotDistinctFromExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *isNotDistinctFromExpr) private(m *memo) interface{} {
	return nil
}

func (e *isNotDistinctFromExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *isNotDistinctFromExpr) isScalar() bool {
	return true
}

func (e *isNotDistinctFromExpr) isLogical() bool {
	return true
}

func (e *isNotDistinctFromExpr) isPhysical() bool {
	return true
}

func (e *isNotDistinctFromExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asIsNotDistinctFrom() *isNotDistinctFromExpr {
	if m.op != isNotDistinctFromOp {
		return nil
	}

	return (*isNotDistinctFromExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIsNotDistinctFrom(e *isNotDistinctFromExpr) groupID {
	const size = uint32(unsafe.Sizeof(isNotDistinctFromExpr{}))
	const align = uint32(unsafe.Alignof(isNotDistinctFromExpr{}))

	isNotDistinctFromOffset := m.lookupExprByFingerprint(e.fingerprint())
	if isNotDistinctFromOffset != 0 {
		return m.lookupExpr(isNotDistinctFromOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*isNotDistinctFromExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type isExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *isExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isExpr{})
	const offset = unsafe.Offsetof(isExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *isExpr) operator() operator {
	return isOp
}

func (e *isExpr) childCount(m *memo) int {
	return 2
}

func (e *isExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *isExpr) private(m *memo) interface{} {
	return nil
}

func (e *isExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *isExpr) isScalar() bool {
	return true
}

func (e *isExpr) isLogical() bool {
	return true
}

func (e *isExpr) isPhysical() bool {
	return true
}

func (e *isExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asIs() *isExpr {
	if m.op != isOp {
		return nil
	}

	return (*isExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIs(e *isExpr) groupID {
	const size = uint32(unsafe.Sizeof(isExpr{}))
	const align = uint32(unsafe.Alignof(isExpr{}))

	isOffset := m.lookupExprByFingerprint(e.fingerprint())
	if isOffset != 0 {
		return m.lookupExpr(isOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*isExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type isNotExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *isNotExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(isNotExpr{})
	const offset = unsafe.Offsetof(isNotExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *isNotExpr) operator() operator {
	return isNotOp
}

func (e *isNotExpr) childCount(m *memo) int {
	return 2
}

func (e *isNotExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *isNotExpr) private(m *memo) interface{} {
	return nil
}

func (e *isNotExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *isNotExpr) isScalar() bool {
	return true
}

func (e *isNotExpr) isLogical() bool {
	return true
}

func (e *isNotExpr) isPhysical() bool {
	return true
}

func (e *isNotExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asIsNot() *isNotExpr {
	if m.op != isNotOp {
		return nil
	}

	return (*isNotExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeIsNot(e *isNotExpr) groupID {
	const size = uint32(unsafe.Sizeof(isNotExpr{}))
	const align = uint32(unsafe.Alignof(isNotExpr{}))

	isNotOffset := m.lookupExprByFingerprint(e.fingerprint())
	if isNotOffset != 0 {
		return m.lookupExpr(isNotOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*isNotExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type anyExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *anyExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(anyExpr{})
	const offset = unsafe.Offsetof(anyExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *anyExpr) operator() operator {
	return anyOp
}

func (e *anyExpr) childCount(m *memo) int {
	return 2
}

func (e *anyExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *anyExpr) private(m *memo) interface{} {
	return nil
}

func (e *anyExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *anyExpr) isScalar() bool {
	return true
}

func (e *anyExpr) isLogical() bool {
	return true
}

func (e *anyExpr) isPhysical() bool {
	return true
}

func (e *anyExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asAny() *anyExpr {
	if m.op != anyOp {
		return nil
	}

	return (*anyExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAny(e *anyExpr) groupID {
	const size = uint32(unsafe.Sizeof(anyExpr{}))
	const align = uint32(unsafe.Alignof(anyExpr{}))

	anyOffset := m.lookupExprByFingerprint(e.fingerprint())
	if anyOffset != 0 {
		return m.lookupExpr(anyOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*anyExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type someExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *someExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(someExpr{})
	const offset = unsafe.Offsetof(someExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *someExpr) operator() operator {
	return someOp
}

func (e *someExpr) childCount(m *memo) int {
	return 2
}

func (e *someExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *someExpr) private(m *memo) interface{} {
	return nil
}

func (e *someExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *someExpr) isScalar() bool {
	return true
}

func (e *someExpr) isLogical() bool {
	return true
}

func (e *someExpr) isPhysical() bool {
	return true
}

func (e *someExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asSome() *someExpr {
	if m.op != someOp {
		return nil
	}

	return (*someExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeSome(e *someExpr) groupID {
	const size = uint32(unsafe.Sizeof(someExpr{}))
	const align = uint32(unsafe.Alignof(someExpr{}))

	someOffset := m.lookupExprByFingerprint(e.fingerprint())
	if someOffset != 0 {
		return m.lookupExpr(someOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*someExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type allExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *allExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(allExpr{})
	const offset = unsafe.Offsetof(allExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *allExpr) operator() operator {
	return allOp
}

func (e *allExpr) childCount(m *memo) int {
	return 2
}

func (e *allExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *allExpr) private(m *memo) interface{} {
	return nil
}

func (e *allExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *allExpr) isScalar() bool {
	return true
}

func (e *allExpr) isLogical() bool {
	return true
}

func (e *allExpr) isPhysical() bool {
	return true
}

func (e *allExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asAll() *allExpr {
	if m.op != allOp {
		return nil
	}

	return (*allExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeAll(e *allExpr) groupID {
	const size = uint32(unsafe.Sizeof(allExpr{}))
	const align = uint32(unsafe.Alignof(allExpr{}))

	allOffset := m.lookupExprByFingerprint(e.fingerprint())
	if allOffset != 0 {
		return m.lookupExpr(allOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*allExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type bitAndExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *bitAndExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(bitAndExpr{})
	const offset = unsafe.Offsetof(bitAndExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *bitAndExpr) operator() operator {
	return bitAndOp
}

func (e *bitAndExpr) childCount(m *memo) int {
	return 2
}

func (e *bitAndExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *bitAndExpr) private(m *memo) interface{} {
	return nil
}

func (e *bitAndExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *bitAndExpr) isScalar() bool {
	return true
}

func (e *bitAndExpr) isLogical() bool {
	return true
}

func (e *bitAndExpr) isPhysical() bool {
	return true
}

func (e *bitAndExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asBitAnd() *bitAndExpr {
	if m.op != bitAndOp {
		return nil
	}

	return (*bitAndExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeBitAnd(e *bitAndExpr) groupID {
	const size = uint32(unsafe.Sizeof(bitAndExpr{}))
	const align = uint32(unsafe.Alignof(bitAndExpr{}))

	bitAndOffset := m.lookupExprByFingerprint(e.fingerprint())
	if bitAndOffset != 0 {
		return m.lookupExpr(bitAndOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*bitAndExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type bitOrExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *bitOrExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(bitOrExpr{})
	const offset = unsafe.Offsetof(bitOrExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *bitOrExpr) operator() operator {
	return bitOrOp
}

func (e *bitOrExpr) childCount(m *memo) int {
	return 2
}

func (e *bitOrExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *bitOrExpr) private(m *memo) interface{} {
	return nil
}

func (e *bitOrExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *bitOrExpr) isScalar() bool {
	return true
}

func (e *bitOrExpr) isLogical() bool {
	return true
}

func (e *bitOrExpr) isPhysical() bool {
	return true
}

func (e *bitOrExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asBitOr() *bitOrExpr {
	if m.op != bitOrOp {
		return nil
	}

	return (*bitOrExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeBitOr(e *bitOrExpr) groupID {
	const size = uint32(unsafe.Sizeof(bitOrExpr{}))
	const align = uint32(unsafe.Alignof(bitOrExpr{}))

	bitOrOffset := m.lookupExprByFingerprint(e.fingerprint())
	if bitOrOffset != 0 {
		return m.lookupExpr(bitOrOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*bitOrExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type bitXorExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *bitXorExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(bitXorExpr{})
	const offset = unsafe.Offsetof(bitXorExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *bitXorExpr) operator() operator {
	return bitXorOp
}

func (e *bitXorExpr) childCount(m *memo) int {
	return 2
}

func (e *bitXorExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *bitXorExpr) private(m *memo) interface{} {
	return nil
}

func (e *bitXorExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *bitXorExpr) isScalar() bool {
	return true
}

func (e *bitXorExpr) isLogical() bool {
	return true
}

func (e *bitXorExpr) isPhysical() bool {
	return true
}

func (e *bitXorExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asBitXor() *bitXorExpr {
	if m.op != bitXorOp {
		return nil
	}

	return (*bitXorExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeBitXor(e *bitXorExpr) groupID {
	const size = uint32(unsafe.Sizeof(bitXorExpr{}))
	const align = uint32(unsafe.Alignof(bitXorExpr{}))

	bitXorOffset := m.lookupExprByFingerprint(e.fingerprint())
	if bitXorOffset != 0 {
		return m.lookupExpr(bitXorOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*bitXorExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type plusExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *plusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(plusExpr{})
	const offset = unsafe.Offsetof(plusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *plusExpr) operator() operator {
	return plusOp
}

func (e *plusExpr) childCount(m *memo) int {
	return 2
}

func (e *plusExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *plusExpr) private(m *memo) interface{} {
	return nil
}

func (e *plusExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *plusExpr) isScalar() bool {
	return true
}

func (e *plusExpr) isLogical() bool {
	return true
}

func (e *plusExpr) isPhysical() bool {
	return true
}

func (e *plusExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asPlus() *plusExpr {
	if m.op != plusOp {
		return nil
	}

	return (*plusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizePlus(e *plusExpr) groupID {
	const size = uint32(unsafe.Sizeof(plusExpr{}))
	const align = uint32(unsafe.Alignof(plusExpr{}))

	plusOffset := m.lookupExprByFingerprint(e.fingerprint())
	if plusOffset != 0 {
		return m.lookupExpr(plusOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*plusExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type minusExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *minusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(minusExpr{})
	const offset = unsafe.Offsetof(minusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *minusExpr) operator() operator {
	return minusOp
}

func (e *minusExpr) childCount(m *memo) int {
	return 2
}

func (e *minusExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *minusExpr) private(m *memo) interface{} {
	return nil
}

func (e *minusExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *minusExpr) isScalar() bool {
	return true
}

func (e *minusExpr) isLogical() bool {
	return true
}

func (e *minusExpr) isPhysical() bool {
	return true
}

func (e *minusExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asMinus() *minusExpr {
	if m.op != minusOp {
		return nil
	}

	return (*minusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeMinus(e *minusExpr) groupID {
	const size = uint32(unsafe.Sizeof(minusExpr{}))
	const align = uint32(unsafe.Alignof(minusExpr{}))

	minusOffset := m.lookupExprByFingerprint(e.fingerprint())
	if minusOffset != 0 {
		return m.lookupExpr(minusOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*minusExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type multExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *multExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(multExpr{})
	const offset = unsafe.Offsetof(multExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *multExpr) operator() operator {
	return multOp
}

func (e *multExpr) childCount(m *memo) int {
	return 2
}

func (e *multExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *multExpr) private(m *memo) interface{} {
	return nil
}

func (e *multExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *multExpr) isScalar() bool {
	return true
}

func (e *multExpr) isLogical() bool {
	return true
}

func (e *multExpr) isPhysical() bool {
	return true
}

func (e *multExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asMult() *multExpr {
	if m.op != multOp {
		return nil
	}

	return (*multExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeMult(e *multExpr) groupID {
	const size = uint32(unsafe.Sizeof(multExpr{}))
	const align = uint32(unsafe.Alignof(multExpr{}))

	multOffset := m.lookupExprByFingerprint(e.fingerprint())
	if multOffset != 0 {
		return m.lookupExpr(multOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*multExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type divExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *divExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(divExpr{})
	const offset = unsafe.Offsetof(divExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *divExpr) operator() operator {
	return divOp
}

func (e *divExpr) childCount(m *memo) int {
	return 2
}

func (e *divExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *divExpr) private(m *memo) interface{} {
	return nil
}

func (e *divExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *divExpr) isScalar() bool {
	return true
}

func (e *divExpr) isLogical() bool {
	return true
}

func (e *divExpr) isPhysical() bool {
	return true
}

func (e *divExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asDiv() *divExpr {
	if m.op != divOp {
		return nil
	}

	return (*divExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeDiv(e *divExpr) groupID {
	const size = uint32(unsafe.Sizeof(divExpr{}))
	const align = uint32(unsafe.Alignof(divExpr{}))

	divOffset := m.lookupExprByFingerprint(e.fingerprint())
	if divOffset != 0 {
		return m.lookupExpr(divOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*divExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type floorDivExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *floorDivExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(floorDivExpr{})
	const offset = unsafe.Offsetof(floorDivExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *floorDivExpr) operator() operator {
	return floorDivOp
}

func (e *floorDivExpr) childCount(m *memo) int {
	return 2
}

func (e *floorDivExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *floorDivExpr) private(m *memo) interface{} {
	return nil
}

func (e *floorDivExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *floorDivExpr) isScalar() bool {
	return true
}

func (e *floorDivExpr) isLogical() bool {
	return true
}

func (e *floorDivExpr) isPhysical() bool {
	return true
}

func (e *floorDivExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asFloorDiv() *floorDivExpr {
	if m.op != floorDivOp {
		return nil
	}

	return (*floorDivExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeFloorDiv(e *floorDivExpr) groupID {
	const size = uint32(unsafe.Sizeof(floorDivExpr{}))
	const align = uint32(unsafe.Alignof(floorDivExpr{}))

	floorDivOffset := m.lookupExprByFingerprint(e.fingerprint())
	if floorDivOffset != 0 {
		return m.lookupExpr(floorDivOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*floorDivExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type modExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *modExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(modExpr{})
	const offset = unsafe.Offsetof(modExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *modExpr) operator() operator {
	return modOp
}

func (e *modExpr) childCount(m *memo) int {
	return 2
}

func (e *modExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *modExpr) private(m *memo) interface{} {
	return nil
}

func (e *modExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *modExpr) isScalar() bool {
	return true
}

func (e *modExpr) isLogical() bool {
	return true
}

func (e *modExpr) isPhysical() bool {
	return true
}

func (e *modExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asMod() *modExpr {
	if m.op != modOp {
		return nil
	}

	return (*modExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeMod(e *modExpr) groupID {
	const size = uint32(unsafe.Sizeof(modExpr{}))
	const align = uint32(unsafe.Alignof(modExpr{}))

	modOffset := m.lookupExprByFingerprint(e.fingerprint())
	if modOffset != 0 {
		return m.lookupExpr(modOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*modExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type powExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *powExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(powExpr{})
	const offset = unsafe.Offsetof(powExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *powExpr) operator() operator {
	return powOp
}

func (e *powExpr) childCount(m *memo) int {
	return 2
}

func (e *powExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *powExpr) private(m *memo) interface{} {
	return nil
}

func (e *powExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *powExpr) isScalar() bool {
	return true
}

func (e *powExpr) isLogical() bool {
	return true
}

func (e *powExpr) isPhysical() bool {
	return true
}

func (e *powExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asPow() *powExpr {
	if m.op != powOp {
		return nil
	}

	return (*powExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizePow(e *powExpr) groupID {
	const size = uint32(unsafe.Sizeof(powExpr{}))
	const align = uint32(unsafe.Alignof(powExpr{}))

	powOffset := m.lookupExprByFingerprint(e.fingerprint())
	if powOffset != 0 {
		return m.lookupExpr(powOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*powExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type concatExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *concatExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(concatExpr{})
	const offset = unsafe.Offsetof(concatExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *concatExpr) operator() operator {
	return concatOp
}

func (e *concatExpr) childCount(m *memo) int {
	return 2
}

func (e *concatExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *concatExpr) private(m *memo) interface{} {
	return nil
}

func (e *concatExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *concatExpr) isScalar() bool {
	return true
}

func (e *concatExpr) isLogical() bool {
	return true
}

func (e *concatExpr) isPhysical() bool {
	return true
}

func (e *concatExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asConcat() *concatExpr {
	if m.op != concatOp {
		return nil
	}

	return (*concatExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeConcat(e *concatExpr) groupID {
	const size = uint32(unsafe.Sizeof(concatExpr{}))
	const align = uint32(unsafe.Alignof(concatExpr{}))

	concatOffset := m.lookupExprByFingerprint(e.fingerprint())
	if concatOffset != 0 {
		return m.lookupExpr(concatOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*concatExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type lShiftExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *lShiftExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(lShiftExpr{})
	const offset = unsafe.Offsetof(lShiftExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *lShiftExpr) operator() operator {
	return lShiftOp
}

func (e *lShiftExpr) childCount(m *memo) int {
	return 2
}

func (e *lShiftExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *lShiftExpr) private(m *memo) interface{} {
	return nil
}

func (e *lShiftExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *lShiftExpr) isScalar() bool {
	return true
}

func (e *lShiftExpr) isLogical() bool {
	return true
}

func (e *lShiftExpr) isPhysical() bool {
	return true
}

func (e *lShiftExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asLShift() *lShiftExpr {
	if m.op != lShiftOp {
		return nil
	}

	return (*lShiftExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeLShift(e *lShiftExpr) groupID {
	const size = uint32(unsafe.Sizeof(lShiftExpr{}))
	const align = uint32(unsafe.Alignof(lShiftExpr{}))

	lShiftOffset := m.lookupExprByFingerprint(e.fingerprint())
	if lShiftOffset != 0 {
		return m.lookupExpr(lShiftOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*lShiftExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type rShiftExpr struct {
	group groupID
	op    operator
	left  groupID
	right groupID
}

func (e *rShiftExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(rShiftExpr{})
	const offset = unsafe.Offsetof(rShiftExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *rShiftExpr) operator() operator {
	return rShiftOp
}

func (e *rShiftExpr) childCount(m *memo) int {
	return 2
}

func (e *rShiftExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	default:
		panic("child index out of range")
	}
}

func (e *rShiftExpr) private(m *memo) interface{} {
	return nil
}

func (e *rShiftExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *rShiftExpr) isScalar() bool {
	return true
}

func (e *rShiftExpr) isLogical() bool {
	return true
}

func (e *rShiftExpr) isPhysical() bool {
	return true
}

func (e *rShiftExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asRShift() *rShiftExpr {
	if m.op != rShiftOp {
		return nil
	}

	return (*rShiftExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeRShift(e *rShiftExpr) groupID {
	const size = uint32(unsafe.Sizeof(rShiftExpr{}))
	const align = uint32(unsafe.Alignof(rShiftExpr{}))

	rShiftOffset := m.lookupExprByFingerprint(e.fingerprint())
	if rShiftOffset != 0 {
		return m.lookupExpr(rShiftOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*rShiftExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type unaryPlusExpr struct {
	group groupID
	op    operator
	input groupID
}

func (e *unaryPlusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(unaryPlusExpr{})
	const offset = unsafe.Offsetof(unaryPlusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *unaryPlusExpr) operator() operator {
	return unaryPlusOp
}

func (e *unaryPlusExpr) childCount(m *memo) int {
	return 1
}

func (e *unaryPlusExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.input
	default:
		panic("child index out of range")
	}
}

func (e *unaryPlusExpr) private(m *memo) interface{} {
	return nil
}

func (e *unaryPlusExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *unaryPlusExpr) isScalar() bool {
	return true
}

func (e *unaryPlusExpr) isLogical() bool {
	return true
}

func (e *unaryPlusExpr) isPhysical() bool {
	return true
}

func (e *unaryPlusExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asUnaryPlus() *unaryPlusExpr {
	if m.op != unaryPlusOp {
		return nil
	}

	return (*unaryPlusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeUnaryPlus(e *unaryPlusExpr) groupID {
	const size = uint32(unsafe.Sizeof(unaryPlusExpr{}))
	const align = uint32(unsafe.Alignof(unaryPlusExpr{}))

	unaryPlusOffset := m.lookupExprByFingerprint(e.fingerprint())
	if unaryPlusOffset != 0 {
		return m.lookupExpr(unaryPlusOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*unaryPlusExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type unaryMinusExpr struct {
	group groupID
	op    operator
	input groupID
}

func (e *unaryMinusExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(unaryMinusExpr{})
	const offset = unsafe.Offsetof(unaryMinusExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *unaryMinusExpr) operator() operator {
	return unaryMinusOp
}

func (e *unaryMinusExpr) childCount(m *memo) int {
	return 1
}

func (e *unaryMinusExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.input
	default:
		panic("child index out of range")
	}
}

func (e *unaryMinusExpr) private(m *memo) interface{} {
	return nil
}

func (e *unaryMinusExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *unaryMinusExpr) isScalar() bool {
	return true
}

func (e *unaryMinusExpr) isLogical() bool {
	return true
}

func (e *unaryMinusExpr) isPhysical() bool {
	return true
}

func (e *unaryMinusExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asUnaryMinus() *unaryMinusExpr {
	if m.op != unaryMinusOp {
		return nil
	}

	return (*unaryMinusExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeUnaryMinus(e *unaryMinusExpr) groupID {
	const size = uint32(unsafe.Sizeof(unaryMinusExpr{}))
	const align = uint32(unsafe.Alignof(unaryMinusExpr{}))

	unaryMinusOffset := m.lookupExprByFingerprint(e.fingerprint())
	if unaryMinusOffset != 0 {
		return m.lookupExpr(unaryMinusOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*unaryMinusExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type unaryComplementExpr struct {
	group groupID
	op    operator
	input groupID
}

func (e *unaryComplementExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(unaryComplementExpr{})
	const offset = unsafe.Offsetof(unaryComplementExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *unaryComplementExpr) operator() operator {
	return unaryComplementOp
}

func (e *unaryComplementExpr) childCount(m *memo) int {
	return 1
}

func (e *unaryComplementExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.input
	default:
		panic("child index out of range")
	}
}

func (e *unaryComplementExpr) private(m *memo) interface{} {
	return nil
}

func (e *unaryComplementExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *unaryComplementExpr) isScalar() bool {
	return true
}

func (e *unaryComplementExpr) isLogical() bool {
	return true
}

func (e *unaryComplementExpr) isPhysical() bool {
	return true
}

func (e *unaryComplementExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asUnaryComplement() *unaryComplementExpr {
	if m.op != unaryComplementOp {
		return nil
	}

	return (*unaryComplementExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeUnaryComplement(e *unaryComplementExpr) groupID {
	const size = uint32(unsafe.Sizeof(unaryComplementExpr{}))
	const align = uint32(unsafe.Alignof(unaryComplementExpr{}))

	unaryComplementOffset := m.lookupExprByFingerprint(e.fingerprint())
	if unaryComplementOffset != 0 {
		return m.lookupExpr(unaryComplementOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*unaryComplementExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type functionExpr struct {
	group groupID
	op    operator
	args  listID
	def   privateID
}

func (e *functionExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(functionExpr{})
	const offset = unsafe.Offsetof(functionExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *functionExpr) operator() operator {
	return functionOp
}

func (e *functionExpr) childCount(m *memo) int {
	return 1 + int(e.args.len)
}

func (e *functionExpr) child(m *memo, n int) groupID {
	switch n {
	default:
		list := m.lookupList(e.args)
		return list[n-0]
	}
}

func (e *functionExpr) private(m *memo) interface{} {
	return nil
}

func (e *functionExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *functionExpr) isScalar() bool {
	return true
}

func (e *functionExpr) isLogical() bool {
	return true
}

func (e *functionExpr) isPhysical() bool {
	return true
}

func (e *functionExpr) isRelational() bool {
	return false
}

func (m *memoExpr) asFunction() *functionExpr {
	if m.op != functionOp {
		return nil
	}

	return (*functionExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeFunction(e *functionExpr) groupID {
	const size = uint32(unsafe.Sizeof(functionExpr{}))
	const align = uint32(unsafe.Alignof(functionExpr{}))

	functionOffset := m.lookupExprByFingerprint(e.fingerprint())
	if functionOffset != 0 {
		return m.lookupExpr(functionOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*functionExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}

type innerJoinExpr struct {
	group  groupID
	op     operator
	left   groupID
	right  groupID
	filter groupID
}

func (e *innerJoinExpr) fingerprint() (f exprFingerprint) {
	const size = unsafe.Sizeof(innerJoinExpr{})
	const offset = unsafe.Offsetof(innerJoinExpr{}.op)

	b := *(*[size]byte)(unsafe.Pointer(e))

	if size-offset <= unsafe.Sizeof(f) {
		copy(f[:], b[offset:])
	} else {
		f = exprFingerprint(md5.Sum(b[offset:]))
	}

	return
}

func (e *innerJoinExpr) operator() operator {
	return innerJoinOp
}

func (e *innerJoinExpr) childCount(m *memo) int {
	return 3
}

func (e *innerJoinExpr) child(m *memo, n int) groupID {
	switch n {
	case 0:
		return e.left
	case 1:
		return e.right
	case 2:
		return e.filter
	default:
		panic("child index out of range")
	}
}

func (e *innerJoinExpr) private(m *memo) interface{} {
	return nil
}

func (e *innerJoinExpr) logicalProps(m *memo) *logicalProps {
	return m.lookupGroup(e.group).props
}

func (e *innerJoinExpr) isScalar() bool {
	return false
}

func (e *innerJoinExpr) isLogical() bool {
	return true
}

func (e *innerJoinExpr) isPhysical() bool {
	return false
}

func (e *innerJoinExpr) isRelational() bool {
	return true
}

func (m *memoExpr) asInnerJoin() *innerJoinExpr {
	if m.op != innerJoinOp {
		return nil
	}

	return (*innerJoinExpr)(unsafe.Pointer(m))
}

func (m *memo) memoizeInnerJoin(e *innerJoinExpr) groupID {
	const size = uint32(unsafe.Sizeof(innerJoinExpr{}))
	const align = uint32(unsafe.Alignof(innerJoinExpr{}))

	innerJoinOffset := m.lookupExprByFingerprint(e.fingerprint())
	if innerJoinOffset != 0 {
		return m.lookupExpr(innerJoinOffset).group
	}

	offset := m.arena.Alloc(size, align)

	if e.group == 0 {
		e.group = m.newGroup(e, exprOffset(offset))
	}

	m.lookupGroup(e.group).addExpr(exprOffset(offset))

	p := (*innerJoinExpr)(m.arena.GetPointer(offset))
	*p = *e

	return e.group
}
