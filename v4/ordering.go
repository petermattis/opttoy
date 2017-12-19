package v4

import (
	"bytes"
	"fmt"
)

// orderSpec defines the order of columns provided or required by a
// relation. A negative value indicates descending order on the column index
// "-(value+1)".
type ordering []colsetIndex

func (o ordering) String() string {
	var buf bytes.Buffer
	o.format(&buf)
	return buf.String()
}

func (o ordering) format(buf *bytes.Buffer) {
	for i, col := range o {
		if i > 0 {
			buf.WriteString(",")
		}
		if col >= 0 {
			fmt.Fprintf(buf, "+%d", col)
		} else {
			fmt.Fprintf(buf, "-%d", -(col + 1))
		}
	}
}

// Provides returns true iff the receiver is a prefix of the required ordering.
func (o ordering) provides(required ordering) bool {
	if len(o) < len(required) {
		return false
	}
	for i := range required {
		if o[i] != required[i] {
			return false
		}
	}
	return true
}
