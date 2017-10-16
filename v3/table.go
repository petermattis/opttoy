package v3

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

type column struct {
	name    string
	notNull bool
}

type foreignKey struct {
	referenced *table
	columns    []int
}

type tableKey struct {
	primary bool
	unique  bool
	notNull bool // all of the columns are notNull
	columns []int
	fkey    *foreignKey
}

func (k *tableKey) equalColumns(other tableKey) bool {
	if len(k.columns) != len(other.columns) {
		return false
	}
	for i := range k.columns {
		if k.columns[i] != other.columns[i] {
			return false
		}
	}
	return true
}

type table struct {
	name    string
	colMap  map[string]int
	columns []column
	keys    []tableKey
}

func createTable(catalog map[string]*table, stmt *parser.CreateTable) *table {
	getKey := func(t *table, key tableKey) *tableKey {
		for i := range t.keys {
			if t.keys[i].equalColumns(key) {
				return &t.keys[i]
			}
		}
		return nil
	}

	addKey := func(t *table, key tableKey) *tableKey {
		existing := getKey(t, key)
		if existing != nil {
			existing.primary = existing.primary || key.primary
			existing.unique = existing.unique || key.unique
			existing.notNull = existing.notNull || key.notNull
			return existing
		}
		key.notNull = true
		for _, i := range key.columns {
			key.notNull = key.notNull && t.columns[i].notNull
		}
		t.keys = append(t.keys, key)
		return &t.keys[len(t.keys)-1]
	}

	addForeignKey := func(src, dest *table, srcColumns, destColumns []int) {
		srcKey := addKey(src, tableKey{columns: srcColumns})
		if srcKey.fkey != nil {
			fatalf("foreign key already defined for %d", srcColumns)
		}
		srcKey.fkey = &foreignKey{
			referenced: dest,
			columns:    destColumns,
		}
	}

	extractColumns := func(def *parser.IndexTableDef) []string {
		res := make([]string, len(def.Columns))
		for i, col := range def.Columns {
			res[i] = string(col.Column)
		}
		return res
	}

	extractNames := func(names parser.NameList) []string {
		res := make([]string, len(names))
		for i, name := range names {
			res[i] = string(name)
		}
		return res
	}

	tableName, err := stmt.Table.Normalize()
	if err != nil {
		fatalf("%s", err)
	}
	name := tableName.Table()
	if _, ok := catalog[name]; ok {
		fatalf("table %s already exists", name)
	}
	tab := &table{
		name:   name,
		colMap: make(map[string]int),
	}
	catalog[name] = tab

	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *parser.ColumnTableDef:
			if _, ok := tab.colMap[string(def.Name)]; ok {
				fatalf("column %s already exists", def.Name)
			}
			index := int(len(tab.columns))
			tab.colMap[string(def.Name)] = index
			tab.columns = append(tab.columns, column{
				name:    string(def.Name),
				notNull: def.PrimaryKey || (def.Nullable.Nullability == parser.NotNull),
			})

			if def.Unique || def.PrimaryKey {
				addKey(tab, tableKey{
					primary: def.PrimaryKey,
					unique:  true,
					columns: []int{index},
				})
			}

			if def.HasFKConstraint() {
				refTable, err := def.References.Table.Normalize()
				if err != nil {
					fatalf("%s", err)
				}
				refName := refTable.Table()
				ref, ok := catalog[refName]
				if !ok {
					fatalf("unable to find referenced table %s", refTable)
				}
				addForeignKey(tab, ref, []int{index},
					ref.getColumnIndexes([]string{string(def.References.Col)}))
			}

		case *parser.UniqueConstraintTableDef:
			columns := tab.getColumnIndexes(extractColumns(&def.IndexTableDef))
			if def.PrimaryKey {
				for _, i := range columns {
					tab.columns[i].notNull = true
				}
			}
			addKey(tab, tableKey{
				primary: def.PrimaryKey,
				unique:  true,
				columns: columns,
			})

		case *parser.IndexTableDef:
			addKey(tab, tableKey{
				unique:  true,
				columns: tab.getColumnIndexes(extractColumns(def)),
			})

		case *parser.ForeignKeyConstraintTableDef:
			refTable, err := def.Table.Normalize()
			if err != nil {
				fatalf("%s", err)
			}
			refName := refTable.Table()
			ref, ok := catalog[refName]
			if !ok {
				fatalf("unable to find referenced table %s", refTable)
			}

			addForeignKey(tab, ref,
				tab.getColumnIndexes(extractNames(def.FromCols)),
				ref.getColumnIndexes(extractNames(def.ToCols)))

		default:
			unimplemented("%T", def)
		}
	}

	return tab
}

func (t *table) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "table %s\n", t.name)
	for _, col := range t.columns {
		fmt.Fprintf(&buf, "  %s", col.name)
		if col.notNull {
			buf.WriteString(" NOT NULL")
		} else {
			buf.WriteString(" NULL")
		}
		buf.WriteString("\n")
	}
	for _, key := range t.keys {
		buf.WriteString("  ")
		buf.WriteString("(")
		for i, colIdx := range key.columns {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(t.columns[colIdx].name)
		}
		buf.WriteString(")")
		if fkey := key.fkey; fkey != nil {
			fmt.Fprintf(&buf, " -> %s(", fkey.referenced.name)
			for i, colIdx := range fkey.columns {
				if i > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(fkey.referenced.columns[colIdx].name)
			}
			buf.WriteString(")")
		}
		if key.unique {
			if key.notNull {
				buf.WriteString(" KEY")
			} else {
				buf.WriteString(" UNIQUE")
			}
		}
		buf.WriteString("\n")
	}
	return buf.String()
}

func (t *table) getColumns(columns []int) []column {
	res := make([]column, len(columns))
	for i, j := range columns {
		res[i] = t.columns[j]
	}
	return res
}

func (t *table) getColumnIndexes(names []string) []int {
	res := make([]int, len(names))
	for i, name := range names {
		index, ok := t.colMap[name]
		if !ok {
			fatalf("unable to find %s.%s", t.name, name)
		}
		res[i] = index
	}
	return res
}
