package opt

import (
	"bytes"
	"fmt"
)

type physicalPropsID uint32

const (
	// defaultPhysPropsID is the id of the set of default properties:
	//   - No ordering
	//   - No columns projected
	defaultPhysPropsID physicalPropsID = 1
)

// Physical properties that can be provided by a relation or required of a
// relation.
type PhysicalProps struct {
	Ordering   Ordering
	Projection Projection
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

func (p *PhysicalProps) Provides(required *PhysicalProps) bool {
	if !p.Projection.Provides(required.Projection) {
		return false
	}

	return p.Ordering.Provides(required.Ordering)
}

// Projection defines the membership and ordering of columns provided or
// required by a relation. If duplicates or ordering is not applicable, then
// the projection can be stored in a more efficient format. The column's
// label can be looked up using the column index.
type Projection struct {
	ordered   []ColumnIndex
	unordered ColSet
}

func NewOrderedProjection(ordered []ColumnIndex) Projection {
	p := Projection{ordered: ordered}

	// Insert the column ids into the unordered set.
	for _, col := range ordered {
		p.unordered.Add(int(col))
	}

	return p
}

func NewUnorderedProjection(unordered ColSet) Projection {
	return Projection{unordered: unordered}
}

// Defined returns true if a subset of the columns provided by the expression
// should be projected. If false, then all columns should be projected.
func (p Projection) Defined() bool {
	return !p.unordered.Empty()
}

func (p Projection) Ordered() bool {
	return p.ordered != nil
}

func (p Projection) OrderedColumns() []ColumnIndex {
	return p.ordered
}

func (p Projection) UnorderedColumns() ColSet {
	return p.unordered
}

func (p Projection) String() string {
	var buf bytes.Buffer
	p.format(&buf)
	return buf.String()
}

func (p Projection) format(buf *bytes.Buffer) {
	if p.ordered != nil {
		for i, col := range p.ordered {
			if i > 0 {
				buf.WriteString(",")
			}

			fmt.Fprintf(buf, "%d", col)
		}
		fmt.Fprint(buf, "*")
	} else {
		first := true
		p.unordered.ForEach(func(i int) {
			if first {
				first = false
			} else {
				buf.WriteString(",")
			}

			fmt.Fprintf(buf, "%d", i)
		})
	}
}

// Provides returns true iff a superset of the required columns are projected.
func (p Projection) Provides(required Projection) bool {
	return required.unordered.SubsetOf(p.unordered)
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
