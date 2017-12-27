package build

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/petermattis/opttoy/v4/cat"
	"github.com/petermattis/opttoy/v4/opt"
)

type columnProps struct {
	name  cat.ColumnName
	table cat.TableName
	typ   types.T
	index opt.ColumnIndex
}

func (c columnProps) String() string {
	if c.table == "" {
		return tree.Name(c.name).String()
	}

	return fmt.Sprintf("%s.%s", tree.Name(c.table), tree.Name(c.name))
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
