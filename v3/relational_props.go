package v3

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// queryState holds per-query state such as the tables referenced by the query
// and the mapping from table name to the column index for those tables columns
// within the query.
type queryState struct {
	catalog map[string]*table
	tables  map[string]bitmapIndex
	nextVar bitmapIndex
}

type columnProps struct {
	name   string
	table  string
	index  bitmapIndex
	hidden bool
}

func (c columnProps) String() string {
	if c.table == "" {
		return tree.Name(c.name).String()
	}
	return fmt.Sprintf("%s.%s", tree.Name(c.table), tree.Name(c.name))
}

func (c columnProps) hasColumn(tableName, colName string) bool {
	if colName != c.name {
		return false
	}
	if tableName == "" {
		return true
	}
	return c.table == tableName
}

func (c columnProps) newVariableExpr(tableName string) *expr {
	if tableName != "" {
		c.table = tableName
	}
	return newVariableExpr(c, c.index)
}

type foreignKeyProps struct {
	src  bitmap
	dest bitmap
}

type relationalProps struct {
	// Output column set.
	outputCols bitmap

	// Columns that are not defined in the underlying expression tree (i.e. not
	// supplied by the inputs to the current expression).
	outerCols bitmap

	// Bitmap indicating which output columns cannot be NULL. The NULL-ability of
	// columns flows from the inputs and can also be derived from filters that
	// are NULL-intolerant.
	notNullCols bitmap

	columns []columnProps

	// A column set is a key if no two rows are equal after projection onto that
	// set. A requirement for a column set to be a key is for no columns in the
	// set to be NULL-able. This requirement stems from the property of NULL
	// where NULL != NULL. The simplest example of a key is the primary key for a
	// table (recall that all of the columns of the primary key are defined to be
	// NOT NULL).
	//
	// A weak key is a set of columns where no two rows containing non-NULL
	// values are equal after projection onto that set. A UNIQUE index on a table
	// is a weak key and possibly a key if all of the columns are NOT NULL. A
	// weak key is a key if "(weakKeys[i] & notNullColumns) == weakKeys[i]".
	weakKeys []bitmap

	// A foreign key is a set of columns in the source table that uniquely
	// identify a single row in the destination table. A foreign key thus refers
	// to a primary key or unique key in the destination table. If the source
	// columns are NOT NULL a foreign key can prove the existence of a row in the
	// destination table and can also be used to infer the cardinality of joins
	// when joining on the foreign key. Consider the schema:
	//
	//   CREATE TABLE departments (
	//     dept_id INT PRIMARY KEY,
	//     name STRING
	//   );
	//
	//   CREATE TABLE employees (
	//     emp_id INT PRIMARY KEY,
	//     dept_id INT REFERENCES d (dept_id),
	//     name STRING,
	//     salary INT
	//   );
	//
	// And the query:
	//
	//   SELECT e.name, e.salary
	//   FROM employees e, departments d
	//   WHERE e.dept_id = d.dept_id
	//
	// The foreign key constraint specifies that employees.dept_id must match a
	// value in departments.dept_id or be NULL. Because departments.dept_id is NOT
	// NULL (due to being part of the primary key), we know the only rows from
	// employees that will not be in the join are those with a NULL dept_id. So we
	// can transform the query into:
	//
	//   SELECT e.name, e.salary
	//   FROM employees e
	//   WHERE e.dept_id IS NOT NULL
	foreignKeys []foreignKeyProps

	// Column equivalency groups. Each entry contains a set of equivalent columns
	// and an entry must contain at least 2 columns. No column may appear in more
	// than one entry.
	equivCols []bitmap

	// The number of joins that have been performed at and below this relation.
	joinDepth int32
}

func (p *relationalProps) init() {
	p.outputCols = bitmap{}
	for _, col := range p.columns {
		p.outputCols.Add(col.index)
	}
}

func (p *relationalProps) String() string {
	var buf bytes.Buffer
	p.format(&buf, 0)
	return buf.String()
}

func (p *relationalProps) format(buf *bytes.Buffer, level int) {
	indent := spaces[:2*level]
	fmt.Fprintf(buf, "%scolumns:", indent)
	for _, col := range p.columns {
		buf.WriteString(" ")
		if col.hidden {
			buf.WriteString("(")
		}
		buf.WriteString(col.table)
		buf.WriteString(".")
		buf.WriteString(col.name)
		buf.WriteString(":")
		fmt.Fprintf(buf, "%d", col.index)
		if p.notNullCols.Contains(col.index) {
			buf.WriteString("*")
		}
		if col.hidden {
			buf.WriteString(")")
		}
	}
	buf.WriteString("\n")
	for _, key := range p.weakKeys {
		var prefix string
		if !key.SubsetOf(p.notNullCols) {
			prefix = "weak "
		}
		fmt.Fprintf(buf, "%s%skey: %s\n", indent, prefix, key)
	}
	for _, fkey := range p.foreignKeys {
		fmt.Fprintf(buf, "%sforeign key: %s -> %s\n", indent, fkey.src, fkey.dest)
	}
	if len(p.equivCols) > 0 {
		fmt.Fprintf(buf, "%sequiv:", indent)
		for _, equiv := range p.equivCols {
			fmt.Fprintf(buf, " %s", equiv)
		}
		buf.WriteString("\n")
	}
}

// fingerprint returns a string which uniquely identifies the relational
// properties within the context of a query.
func (p *relationalProps) fingerprint() string {
	// TODO(peter): The fingerprint is unique, but human readable. A binary
	// format encoding columns and keys using varints might be faster and more
	// compact.
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[%s %s %s]", p.outputCols, p.outerCols, p.notNullCols)
	if len(p.weakKeys) > 0 {
		buf.WriteString(" [")
		for i, key := range p.weakKeys {
			if i > 0 {
				buf.WriteString(" ")
			}
			fmt.Fprintf(&buf, "%s", key)
		}
		buf.WriteString("]")
	}
	if len(p.foreignKeys) > 0 {
		buf.WriteString(" [")
		for i, fkey := range p.foreignKeys {
			if i > 0 {
				buf.WriteString(" ")
			}
			fmt.Fprintf(&buf, "%s->%s", fkey.src, fkey.dest)
		}
		buf.WriteString("]")
	}
	if p.joinDepth > 0 {
		fmt.Fprintf(&buf, " %d", p.joinDepth)
	}
	return buf.String()
}

func (p *relationalProps) findColumn(name string) *columnProps {
	for i := range p.columns {
		col := &p.columns[i]
		if col.name == name {
			return col
		}
	}
	return nil
}

func (p *relationalProps) findColumnByIndex(index bitmapIndex) *columnProps {
	for i := range p.columns {
		col := &p.columns[i]
		if col.index == index {
			return col
		}
	}
	return nil
}

// Add additional not-NULL columns based on the filtering expressions.
func (p *relationalProps) applyFilters(filters []*expr) {
	// Expand the set of non-NULL columns based on the filters.
	//
	// TODO(peter): Need to make sure the filter is not null-tolerant.
	for _, filter := range filters {
		p.notNullCols.UnionWith(filter.scalarInputCols())
	}

	// Find equivalent columns.
	p.equivCols = nil
	for _, filter := range filters {
		if filter.op == eqOp {
			left := filter.children[0]
			right := filter.children[1]
			if left.op == variableOp && right.op == variableOp {
				v := left.scalarProps.inputCols
				v.UnionWith(right.scalarProps.inputCols)
				p.addEquivColumns(v)
			}
			// TODO(peter): Support tuple comparisons such as "(a, b) = (c, d)".
		}
	}
}

func (p *relationalProps) applyInputs(inputs []*expr) {
	// Propagate equivalent columns from inputs.
	for _, input := range inputs {
		for _, equiv := range input.props.equivCols {
			p.addEquivColumns(equiv)
		}
	}
}

func (p *relationalProps) addEquivColumns(v bitmap) {
	for i, equiv := range p.equivCols {
		if v.Intersects(equiv) {
			p.equivCols[i].UnionWith(v)
			return
		}
	}
	p.equivCols = append(p.equivCols, v)
}

func initKeys(e *expr, state *queryState) {
	for _, input := range e.inputs() {
		initKeys(input, state)
	}
	e.initKeys(state)
}

func updateProps(e *expr) {
	for _, child := range e.children {
		if child != nil {
			updateProps(child)
		}
	}
	e.updateProps()
}
