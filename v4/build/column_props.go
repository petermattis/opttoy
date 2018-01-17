package build

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/petermattis/opttoy/v4/cat"
	"github.com/petermattis/opttoy/v4/opt"
)

// columnProps holds per-column information that is scoped to a particular
// relational expression. Note that columnProps implements the tree.TypedExpr
// interface. During name resolution, unresolved column names in the AST are
// replaced with a columnProps.
type columnProps struct {
	name   cat.ColumnName
	table  cat.TableName
	typ    types.T
	index  opt.ColumnIndex
	hidden bool
}

var _ tree.TypedExpr = &columnProps{}
var _ tree.VariableExpr = &columnProps{}

func (c columnProps) String() string {
	if c.table == "" {
		return tree.NameString(string(c.name))
	}
	return fmt.Sprintf("%s.%s",
		tree.NameString(string(c.table)), tree.NameString(string(c.name)))
}

func (c columnProps) matches(tblName cat.TableName, colName cat.ColumnName) bool {
	if colName != c.name {
		return false
	}
	if tblName == "" {
		return true
	}
	return c.table == tblName
}
