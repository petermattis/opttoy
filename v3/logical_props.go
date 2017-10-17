package v3

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// queryState holds per-query state such as the tables referenced by the query
// and the mapping from table name to the column index for those tables columns
// within the query.
type queryState struct {
	catalog map[string]*table
	tables  map[string]bitmapIndex
	nextVar bitmapIndex
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

type columnProps struct {
	name   string
	tables []string
	index  bitmapIndex
	// TODO(peter): value constraints.
}

func (c columnProps) hasColumn(tableName, colName string) bool {
	if colName != c.name {
		return false
	}
	if tableName == "" {
		return true
	}
	return c.hasTable(tableName)
}

func (c columnProps) hasTable(tableName string) bool {
	for _, t := range c.tables {
		if t == tableName {
			return true
		}
	}
	return false
}

func (c columnProps) resolvedName(tableName string) *parser.ColumnItem {
	if tableName == "" {
		if len(c.tables) > 0 {
			tableName = c.tables[0]
		}
	}
	return &parser.ColumnItem{
		TableName: parser.TableName{
			TableName:               parser.Name(tableName),
			DBNameOriginallyOmitted: true,
		},
		ColumnName: parser.Name(c.name),
	}
}

func (c columnProps) newVariableExpr(tableName string, props *logicalProps) *expr {
	e := &expr{
		op:        variableOp,
		dataIndex: props.state.addData(c.resolvedName(tableName)),
		props:     props,
	}
	e.inputVars.set(c.index)
	e.updateProperties()
	return e
}

type logicalProps struct {
	columns []columnProps
	// Bitmap indicating which output columns cannot be NULL. The NULL-ability of
	// columns flows from the inputs and can also be derived from filters that
	// are NULL-intolerant.
	notNullCols bitmap
	// TODO(peter): Bitmap indicating which output columns are constant.
	// constCols bitmap

	// A column set is a key if no two rows are equal after projection onto that
	// set. A requirement for a column set to be a key is for no columns in the
	// set to be NULL-able. This requirement stems from the property of NULL
	// where NULL != NULL. The simplest example of a key is the primary key for a
	// table (recall that all of the columns of the primary key are defined to be
	// NOT NULL).
	//
	// A candidate key is a set of columns where no two rows containing non-NULL
	// values are equal after projection onto that set. A UNIQUE index on a table
	// is a candidate key and possibly a key if all of the columns are NOT
	// NULL. A candidate key is a key if "(candidateKeys[i] & notNullColumns) ==
	// candidateKeys[i]".
	candidateKeys []bitmap
	// The global query state.
	state *queryState
}

func (t *logicalProps) String() string {
	var buf bytes.Buffer
	var outputVars bitmap
	for i, col := range t.columns {
		if i > 0 {
			buf.WriteString(" ")
		}
		if tables := col.tables; len(tables) > 1 {
			buf.WriteString("{")
			for j, table := range tables {
				if j > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(table)
			}
			buf.WriteString("}")
		} else if len(tables) == 1 {
			buf.WriteString(tables[0])
		}
		buf.WriteString(".")
		buf.WriteString(col.name)
		buf.WriteString(":")
		fmt.Fprintf(&buf, "%d", col.index)
		outputVars |= 1 << col.index
	}
	for _, key := range t.candidateKeys {
		buf.WriteString(" ")
		if (key & t.notNullCols) == key {
			buf.WriteString("*")
		}
		fmt.Fprintf(&buf, "(%s)", key)
	}
	if t.notNullCols != 0 {
		fmt.Fprintf(&buf, " ![%s]", t.notNullCols)
	}
	return buf.String()
}

func (t *logicalProps) newColumnExpr(name string) *expr {
	for _, col := range t.columns {
		if col.name == name {
			return col.newVariableExpr(col.tables[0], t)
		}
	}
	return nil
}

func concatLogicalProperties(left, right *logicalProps) *logicalProps {
	t := &logicalProps{
		columns: make([]columnProps, len(left.columns)+len(right.columns)),
		state:   left.state,
	}
	copy(t.columns[:], left.columns)
	copy(t.columns[len(left.columns):], right.columns)
	return t
}
