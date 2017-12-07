package v3

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func unimplemented(format string, args ...interface{}) {
	panic("unimplemented: " + fmt.Sprintf(format, args...))
}

func fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type planner struct {
	catalog map[tableName]*table
}

func newPlanner() *planner {
	return &planner{
		catalog: make(map[tableName]*table),
	}
}

func (p *planner) exec(stmt tree.Statement) string {
	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		tab := createTable(p.catalog, stmt)
		return tab.String()
	case *tree.Insert:
		name, ok := stmt.Table.(*tree.NormalizableTableName)
		if !ok {
			unimplemented("%T", stmt)
		}
		tname, err := name.Normalize()
		if err != nil {
			fatalf("unable to normalize: %v", err)
		}
		if tname.PrefixName != "histogram" {
			unimplemented("%s", stmt)
		}
		// This is a statement of the form
		//   INSERT INTO histogram.table.column VALUES ...
		//
		// The histogram.table.column tokens map to
		// PrefixName.DatabaseName.TableName. So we get the table name from
		// DatabaseName and the column name from TableName.
		h := createHistogram(
			p.catalog, tableName(tname.DatabaseName), columnName(tname.TableName), stmt.Rows,
		)
		return h.String()
	case *tree.Select:
		return filterHistogram(p.catalog, stmt).String()
	default:
		unimplemented("%T", stmt)
	}
	return ""
}

func (p *planner) build(stmt tree.Statement) *expr {
	state := &queryState{
		catalog: p.catalog,
		tables:  make(map[tableName]bitmapIndex),
	}
	e := build(stmt, &scope{
		props: &relationalProps{},
		state: state,
	})
	updateProps(e)
	initKeys(e, state)
	return e
}
