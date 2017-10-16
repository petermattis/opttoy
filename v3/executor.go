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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func unimplemented(format string, args ...interface{}) {
	panic("unimplemented: " + fmt.Sprintf(format, args...))
}

func fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type columnRef struct {
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
	data    []interface{}
}

func (s *queryState) addData(d interface{}) int32 {
	s.data = append(s.data, d)
	return int32(len(s.data))
}

func (s *queryState) getData(idx int32) interface{} {
	if idx == 0 {
		return nil
	}
	return s.data[idx-1]
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
			expr := e.prep(stmt)
			pushDownFilters(expr)
			fmt.Printf("%s\n", expr)
		}
	}
}

func (e *executor) prep(stmt parser.Statement) *expr {
	return build(stmt, &table{
		state: &queryState{
			catalog: e.catalog,
			tables:  make(map[string]bitmapIndex),
		},
	})
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
		name: name,
	}
	e.catalog[name] = table
	tables := []string{name}

	columns := make(map[string]struct{})
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *parser.ColumnTableDef:
			if _, ok := columns[string(def.Name)]; ok {
				fatalf("column %s already exists", def.Name)
			}
			columns[string(def.Name)] = struct{}{}
			table.columns = append(table.columns, column{
				name:   string(def.Name),
				tables: tables,
			})
		default:
			unimplemented("%T", def)
		}
	}
}
