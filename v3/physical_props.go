package v3

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Physical properties that can be provided by a relation.
type physicalProps struct {
	providedOrdering ordering
}

func (p *physicalProps) format(tp treeprinter.Node) {
	tp.Childf("ordering: %s", p.providedOrdering.String())
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
