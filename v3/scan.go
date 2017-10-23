package v3

import (
	"bytes"
)

func init() {
	registerOperator(scanOp, "scan", scan{})
}

type scan struct{}

func (scan) format(e *expr, buf *bytes.Buffer, level int) {
	formatRelational(e, buf, level)
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (s scan) updateProps(e *expr) {
	tab := e.private.(*table)
	props := e.props
	if props.columns == nil {
		props.columns = make([]columnProps, 0, len(tab.columns))

		state := props.state
		base, ok := state.tables[tab.name]
		if !ok {
			base = state.nextVar
			state.tables[tab.name] = base
			state.nextVar += bitmapIndex(len(tab.columns))
		}

		tables := []string{tab.name}
		for i, col := range tab.columns {
			index := base + bitmapIndex(i)
			props.columns = append(props.columns, columnProps{
				index:  index,
				name:   col.name,
				tables: tables,
			})
		}
	}

	s.updateKeys(e)

	// Initialize not-NULL columns from the table schema.
	for i, col := range tab.columns {
		if col.notNull {
			props.notNullCols |= 1 << props.columns[i].index
		}
	}

	e.inputVars = 0
	for _, filter := range e.filters() {
		e.inputVars |= filter.inputVars
	}
	e.inputVars &^= e.props.outputVars()

	props.applyFilters(e.filters())
}

func (scan) updateKeys(e *expr) {
	tab := e.private.(*table)
	props := e.props

	if props.weakKeys == nil {
		for _, k := range tab.keys {
			if k.fkey == nil && (k.primary || k.unique) {
				var key bitmap
				for _, i := range k.columns {
					key.set(props.columns[i].index)
				}
				props.weakKeys = append(props.weakKeys, key)
			}
		}
	}

	props.foreignKeys = nil
	for _, k := range tab.keys {
		if k.fkey != nil {
			base, ok := props.state.tables[k.fkey.referenced.name]
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
