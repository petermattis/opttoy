package cat

import (
	"bytes"
	"fmt"
)

var implicitPrimaryKey = &TableKey{Name: "primary", Primary: true}

type TableName string

type Table struct {
	Name    TableName
	Columns []Column
	Keys    []TableKey

	// colMap indexes all columns by mapping their name to their ordinal
	// position in the table.
	colMap map[ColumnName]ColumnOrdinal
}

func (t *Table) AddColumn(col *Column) ColumnOrdinal {
	if t.colMap == nil {
		t.colMap = make(map[ColumnName]ColumnOrdinal)
	}

	ord, ok := t.colMap[col.Name]
	if ok {
		fatalf("table '%s' already has column '%s'", t.Name, col.Name)
	}

	ord = ColumnOrdinal(len(t.Columns))
	t.Columns = append(t.Columns, *col)
	t.colMap[col.Name] = ord
	return ord
}

func (t *Table) AddKey(key *TableKey) *TableKey {
	for i := range t.Keys {
		existing := &t.Keys[i]
		if existing.Name == key.Name {
			fatalf("table '%s' already has key '%s'", t.Name, key.Name)
		}
	}

	t.Keys = append(t.Keys, *key)
	return &t.Keys[len(t.Keys)-1]
}

func (t *Table) Column(name ColumnName) *Column {
	ord := t.ColumnOrdinal(name)
	return &t.Columns[ord]
}

func (t *Table) ColumnOrdinal(name ColumnName) ColumnOrdinal {
	if t.colMap != nil {
		ord, ok := t.colMap[name]
		if ok {
			return ord
		}
	}

	fatalf("column name '%s' not found in table '%s'", name, t.Name)
	return 0
}

func (t *Table) PrimaryKey() *TableKey {
	for i := range t.Keys {
		k := &t.Keys[i]
		if k.Primary {
			return k
		}
	}

	return implicitPrimaryKey
}

func (t *Table) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "table %s\n", t.Name)
	for _, col := range t.Columns {
		fmt.Fprintf(&buf, "  %s", col.Name)
		if col.NotNull {
			buf.WriteString(" NOT NULL")
		} else {
			buf.WriteString(" NULL")
		}
		buf.WriteString("\n")
	}

	for _, key := range t.Keys {
		buf.WriteString("  ")
		buf.WriteString("(")
		for i, colIdx := range key.Columns {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(string(t.Columns[colIdx].Name))
		}
		buf.WriteString(")")

		if fkey := key.Fkey; fkey != nil {
			fmt.Fprintf(&buf, " -> %s(", fkey.Referenced.Name)
			for i, colIdx := range fkey.Columns {
				if i > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(string(fkey.Referenced.Columns[colIdx].Name))
			}
			buf.WriteString(")")
		}

		if key.Unique {
			if key.NotNull {
				buf.WriteString(" KEY")
			} else {
				buf.WriteString(" WEAK KEY")
			}
		}

		buf.WriteString("\n")
	}

	return buf.String()
}

type TableKey struct {
	Name    string
	Primary bool
	Unique  bool
	NotNull bool
	Columns []ColumnOrdinal
	Fkey    *ForeignKey
}

func (k *TableKey) EqualColumns(other *TableKey) bool {
	if len(k.Columns) != len(other.Columns) {
		return false
	}
	for i := range k.Columns {
		if k.Columns[i] != other.Columns[i] {
			return false
		}
	}
	return true
}

type ForeignKey struct {
	Referenced *Table
	Columns    []ColumnOrdinal
}
