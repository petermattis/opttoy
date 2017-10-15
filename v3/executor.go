// TODO(peter):
//
// logical properties
// - required columns
// - derived columns
// - functional dependencies
// - column value constraints
//
// cost-agnostic transformations
// - predicate push down
// - join elimination
// - unnesting
//
// cost-based transformations
// - join re-ordering
// - group-by pull-up
// - group-by push-down

package v3

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func unimplemented(format string, args ...interface{}) {
	panic("unimplemented: " + fmt.Sprintf(format, args...))
}

func fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type columnIndex uint

type table struct {
	name        string
	columns     map[string]columnIndex
	columnNames []string
}

func (t *table) String() string {
	return fmt.Sprintf("%s (%s)", t.name, strings.Join(t.columnNames, ", "))
}

type columnRef struct {
	// TODO(peter): rather than a table, this should be a relation so that column
	// references can refer to intermediate results in the query.
	table *table
	index columnIndex
}

// queryState holds per-query state such as the tables referenced by the query
// and the mapping from table name to the column index for those tables columns
// within the query.
type queryState struct {
	catalog map[string]*table
	tables  map[string]bitmapIndex
	// query index to table and column index.
	columns []columnRef
}

type executor struct {
	catalog map[string]*table
}

func newExecutor() *executor {
	return &executor{
		catalog: make(map[string]*table),
	}
}

func (e *executor) exec(sql string) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		panic(err)
	}
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *parser.CreateTable:
			e.createTable(stmt)
		default:
			fmt.Printf("%s\n", stmt)
			expr, _ := e.prep(stmt)
			pushDownFilters(expr)
			fmt.Printf("%s\n", expr)
		}
	}
}

func (e *executor) prep(stmt parser.Statement) (*expr, *queryState) {
	state := &queryState{
		catalog: e.catalog,
		tables:  make(map[string]bitmapIndex),
	}
	expr, _ := build(stmt, state, nil)
	return expr, state
}

func (e *executor) createTable(stmt *parser.CreateTable) {
	tableName, err := stmt.Table.Normalize()
	if err != nil {
		fatalf("%s", err)
	}
	name := tableName.Table()
	if _, ok := e.catalog[name]; ok {
		fatalf("table %s already exists", name)
	}
	table := &table{
		name:    name,
		columns: make(map[string]columnIndex),
	}
	e.catalog[name] = table

	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *parser.ColumnTableDef:
			if _, ok := table.columns[string(def.Name)]; ok {
				fatalf("column %s already exists", def.Name)
			}
			table.columns[string(def.Name)] = columnIndex(len(table.columnNames))
			table.columnNames = append(table.columnNames, string(def.Name))
		default:
			unimplemented("%T", def)
		}
	}
}
