package v4

import (
	"bytes"
)

// Physical properties that can be provided by a relation.
type physicalProps struct {
	cols  colset
	order ordering
}

func (p *physicalProps) format(tp *treePrinter) {
	tp.Addf("cols: %s, ordering: %s", p.cols.String(), p.order.String())
}

func (p *physicalProps) String() string {
	if p == nil || len(p.order) == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString("<")
	p.order.format(&buf)
	buf.WriteString(">")
	return buf.String()
}
