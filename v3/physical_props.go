package v3

import (
	"bytes"
)

// Physical properties that can be provided by a relation.
type physicalProps struct {
	providedOrdering ordering
}

func (p *physicalProps) format(buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	buf.WriteString(indent)
	buf.WriteString("ordering: ")
	p.providedOrdering.format(buf)
	buf.WriteString("\n")
}

func (p *physicalProps) fingerprint() string {
	if p == nil || len(p.providedOrdering) == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("<")
	p.providedOrdering.format(&buf)
	buf.WriteString(">")
	return buf.String()
}

func (p *physicalProps) provides(required *physicalProps) bool {
	if required == nil || len(required.providedOrdering) == 0 {
		return true
	}
	if p == nil {
		return false
	}
	return p.providedOrdering.provides(required.providedOrdering)
}
