package opt

import (
	"bytes"
	"fmt"
)

type physicalPropsID uint32

const (
	// defaultPhysPropsID is the id of the set of default properties:
	//   - No ordering
	//   - No special ordering or naming of columns
	defaultPhysPropsID physicalPropsID = 1
)

// Physical properties that can be provided by a relation or required of a
// relation.
type PhysicalProps struct {
	Ordering   Ordering
	Projection Projection
}

func (p *PhysicalProps) Defined() bool {
	return p.Ordering.Defined() || p.Projection.Defined()
}

func (p *PhysicalProps) fingerprint() string {
	hasOrdering := p.Ordering.Defined()
	hasProjection := p.Projection.Defined()

	// Handle default properties case.
	if !hasOrdering && !hasProjection {
		return ""
	}

	var buf bytes.Buffer

	if hasOrdering {
		buf.WriteString("o:")
		p.Ordering.format(&buf)

		if hasProjection {
			buf.WriteString(" ")
		}
	}

	if hasProjection {
		buf.WriteString("p:")
		p.Projection.format(&buf)
	}

	return buf.String()
}

// Projection defines the ordering and naming of columns provided or required
// by a relation.
type Projection struct {
	Columns []LabeledColumn
}

func (p Projection) Defined() bool {
	return p.Columns != nil
}

func (p Projection) String() string {
	var buf bytes.Buffer
	p.format(&buf)
	return buf.String()
}

func (p Projection) format(buf *bytes.Buffer) {
	for i, col := range p.Columns {
		if i > 0 {
			buf.WriteString(",")
		}

		fmt.Fprintf(buf, "%s:%d", col.Label, col.Index)
	}
}

type LabeledColumn struct {
	Label string
	Index ColumnIndex
}

// Ordering defines the order of columns provided or required by a relation.
// A negative value indicates descending order on the column index "-(value)".
type Ordering []ColumnIndex

func (o Ordering) Defined() bool {
	return len(o) != 0
}

func (o Ordering) String() string {
	var buf bytes.Buffer
	o.format(&buf)
	return buf.String()
}

func (o Ordering) format(buf *bytes.Buffer) {
	for i, col := range o {
		if i > 0 {
			buf.WriteString(",")
		}
		if col >= 0 {
			fmt.Fprintf(buf, "+%d", col)
		} else {
			fmt.Fprintf(buf, "-%d", -col)
		}
	}
}

// Provides returns true iff the receiver is a prefix of the required ordering.
func (o Ordering) Provides(required Ordering) bool {
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
