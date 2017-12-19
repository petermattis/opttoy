package v4

import (
	"math"
	"unsafe"
)

// arena is single-threaded.
type arena struct {
	n   uint32
	buf []byte
}

// NewArena allocates a new arena of the specified size and returns it.
func newArena(size uint32) *arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &arena{
		n:   1,
		buf: make([]byte, size),
	}

	return out
}

func (a *arena) Size() uint32 {
	return a.n
}

// Alloc always aligns allocations on pointer-aligned boundary.
func (a *arena) Alloc(size, align uint32) uint32 {
	start := (uint64(a.n) + (uint64(align) - 1)) & ^(uint64(align) - 1)
	end := start + uint64(size)

	if end > uint64(len(a.buf)) {
		// Resize buffer.
		newSize := end * 2
		if newSize > math.MaxUint32 {
			panic("buffer exceeded maximum size")
		}

		newBuf := make([]byte, newSize)
		copy(newBuf[:a.n], a.buf[:a.n])
		a.buf = newBuf
	}

	a.n = uint32(end)
	return uint32(start)
}

func (a *arena) GetBytes(offset uint32, size uint32) []byte {
	if offset == 0 {
		return nil
	}

	return a.buf[offset : offset+size]
}

func (a *arena) GetPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}

	return unsafe.Pointer(&a.buf[offset])
}

func (a *arena) GetPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}

	offset := uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0]))
	if offset < 1 || offset >= uintptr(len(a.buf)) {
		panic("ptr cannot point outside the arena")
	}

	return uint32(offset)
}
