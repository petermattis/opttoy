package v3

import (
	"bytes"
	"fmt"
)

// Bitmap used for columns. We're limited to using 64 in a query due to
// laziness. Use FastIntSet in a real implementation.
type bitmap uint64
type bitmapIndex uint32

func (b bitmap) String() string {
	appendBitmapRange := func(buf *bytes.Buffer, start, end int) {
		if buf.Len() > 0 {
			fmt.Fprintf(buf, ",")
		}
		if start == end {
			fmt.Fprintf(buf, "%d", start)
		} else {
			fmt.Fprintf(buf, "%d-%d", start, end)
		}
	}

	var buf bytes.Buffer
	start := -1
	for i := 0; i < 64; i++ {
		if b.get(bitmapIndex(i)) {
			if start == -1 {
				start = i
			}
		} else if start != -1 {
			appendBitmapRange(&buf, start, i-1)
			start = -1
		}
	}
	if start != -1 {
		appendBitmapRange(&buf, start, 63)
	}
	return buf.String()
}

func (b bitmap) get(i bitmapIndex) bool {
	return b&(1<<uint(i)) != 0
}

func (b *bitmap) set(i bitmapIndex) {
	*b |= 1 << i
}

func (b *bitmap) clear(i bitmapIndex) {
	*b &^= 1 << i
}
