package v3

import (
	"bytes"
)

func init() {
	registerOperator(scanOp, "scan", scan{})
}

func newScanExpr(tab *table) *expr {
	return &expr{
		op:       scanOp,
		extra:    1,
		children: []*expr{nil /* filter */},
		props:    &relationalProps{},
		private:  tab,
	}
}

type scan struct{}

func (scan) kind() operatorKind {
	return relationalKind
}

func (scan) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (scan) initKeys(e *expr, state *queryState) {
	tab := e.private.(*table)
	props := e.props
	props.foreignKeys = nil

	for _, k := range tab.keys {
		if k.fkey != nil {
			base, ok := state.tables[k.fkey.referenced.name]
			if !ok {
				// The referenced table is not part of the query.
				continue
			}

			var src bitmap
			for _, i := range k.columns {
				src.set(props.columns[i].index)
			}
			var dest bitmap
			for _, i := range k.fkey.columns {
				dest.set(base + bitmapIndex(i))
			}

			props.foreignKeys = append(props.foreignKeys, foreignKeyProps{
				src:  src,
				dest: dest,
			})
		}
	}
}

func (s scan) updateProps(e *expr) {
	e.inputVars = s.requiredInputVars(e)
	e.inputVars &^= e.props.outputVars

	e.props.applyFilters(e.filters())
}

func (scan) requiredInputVars(e *expr) bitmap {
	var v bitmap
	for _, filter := range e.filters() {
		v |= filter.inputVars
	}
	return v
}
