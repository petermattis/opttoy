package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/petermattis/opttoy/v4/cat"
)

type Engine struct {
	catalog *cat.Catalog
}

func NewEngine(catalog *cat.Catalog) *Engine {
	return &Engine{catalog: catalog}
}

func (e *Engine) Catalog() *cat.Catalog {
	return e.catalog
}

func (e *Engine) Execute(stmt tree.Statement) string {
	if stmt.StatementType() != tree.DDL {
		fatalf("statement type is not DDL: %v", stmt.StatementType())
	}

	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		ct := createTable{catalog: e.catalog}
		tbl := ct.execute(stmt)
		return tbl.String()

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
		ch := createHistogram{catalog: e.catalog}
		h := ch.execute(cat.TableName(tname.DatabaseName), cat.ColumnName(tname.TableName), stmt.Rows)
		return h.String()

	case *tree.Select:
		sel, ok := stmt.Select.(*tree.SelectClause)
		if !ok {
			unimplemented("%s", stmt)
		}

		// Get the histogram name.
		name, ok := sel.From.Tables[0].(*tree.AliasedTableExpr).Expr.(*tree.NormalizableTableName)
		if !ok {
			unimplemented("%s", stmt)
		}

		tname, err := name.Normalize()
		if err != nil {
			fatalf("unable to normalize: %v", err)
		}

		if tname.PrefixName != "histogram" {
			unimplemented("%s", stmt)
		}

		fh := filterHistogram{catalog: e.catalog}
		h := fh.execute(cat.TableName(tname.DatabaseName), cat.ColumnName(tname.TableName), sel)
		return h.String()

	default:
		unimplemented("%T", stmt)
	}

	return ""
}
