package v3

import (
	"bytes"
	"fmt"
)

func init() {
	registerOperator(scanOp, "scan", scan{})
}

type scan struct{}

func (scan) format(e *expr, buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%s%v (%s)", indent, e.op, e.props)
	e.formatVars(buf)
	buf.WriteString("\n")
	formatExprs(buf, "filters", e.filters(), level)
	formatExprs(buf, "inputs", e.inputs(), level)
}

func (scan) updateProperties(e *expr) {
	tab := e.props.state.getData(e.dataIndex).(*table)
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

		for _, k := range tab.keys {
			if k.primary || k.unique {
				var key bitmap
				for _, i := range k.columns {
					key |= 1 << props.columns[i].index
				}
				props.candidateKeys = append(props.candidateKeys, key)
			}
		}
	}

	// Initialize not-NULL columns from the table schema.
	for i, col := range tab.columns {
		if col.notNull {
			props.notNullCols |= 1 << props.columns[i].index
		}
	}

	props.applyFilters(e.filters())

	e.inputVars = 0
	for _, col := range e.props.columns {
		e.inputVars.set(col.index)
	}
	e.outputVars = e.inputVars
}
