package v4

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

type logicalProps struct {
	scalar     scalarProps
	relational relationalProps
}

type scalarProps struct {
	// Columns used directly or indirectly by the scalar expression.
	inputCols colset
}

// metadata holds schema data that was compiled specifically for a particular
// query, and is therefore only valid for that one query.
type metadata struct {
	// map from table name to table
	catalog map[tableName]*table
	// map from table name to the column index for the table's columns within the
	// query (they form a contiguous group starting at this index).
	//
	// TODO(peter): This is used to lookup tables for foreign keys, but such
	// lookups need to be scoped. Or the handling of foreign keys needs to be
	// rethought.
	tables map[tableName]colsetIndex
}

type foreignKeyProps struct {
	src  colset
	dest colset
}

type relationalProps struct {
	// Output column set.
	outputCols colset

	// Columns that are not defined in the underlying expression tree (i.e. not
	// supplied by the inputs to the current expression).
	outerCols colset

	// Bitmap indicating which output columns cannot be NULL. The NULL-ability of
	// columns flows from the inputs and can also be derived from filters that
	// are NULL-intolerant.
	notNullCols colset

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
	weakKeys []colset

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
	equivCols []colset
}

func (p *relationalProps) String() string {
	tp := treeprinter.New()
	p.format(tp)
	return tp.String()
}

func (p *relationalProps) format(tp treeprinter.Node) {
	var buf bytes.Buffer
	buf.WriteString("columns:")
	for _, col := range p.columns {
		buf.WriteString(" ")
		if col.hidden {
			buf.WriteString("(")
		}
		buf.WriteString(string(col.table))
		buf.WriteString(".")
		buf.WriteString(string(col.name))
		buf.WriteString(":")
		fmt.Fprintf(&buf, "%d", col.index)
		if p.notNullCols.Contains(col.index) {
			buf.WriteString("*")
		}
		if col.hidden {
			buf.WriteString(")")
		}
	}
	tp.Child(buf.String())
	for _, key := range p.weakKeys {
		var prefix string
		if !key.SubsetOf(p.notNullCols) {
			prefix = "weak "
		}
		tp.Childf("%skey: %s", prefix, key)
	}
	for _, fkey := range p.foreignKeys {
		tp.Childf("foreign key: %s -> %s", fkey.src, fkey.dest)
	}
	if len(p.equivCols) > 0 {
		var buf bytes.Buffer
		buf.WriteString("equiv:")
		for _, equiv := range p.equivCols {
			fmt.Fprintf(&buf, " %s", equiv)
		}
		tp.Child(buf.String())
	}
}

func (p *relationalProps) findColumn(name columnName) *columnProps {
	for i := range p.columns {
		col := &p.columns[i]
		if col.name == name {
			return col
		}
	}
	return nil
}

func (p *relationalProps) findColumnByIndex(index colsetIndex) *columnProps {
	for i := range p.columns {
		col := &p.columns[i]
		if col.index == index {
			return col
		}
	}
	return nil
}

func (p *relationalProps) addEquivColumns(cols colset) {
	for i, equiv := range p.equivCols {
		if cols.Intersects(equiv) {
			p.equivCols[i].UnionWith(cols)
			return
		}
	}

	p.equivCols = append(p.equivCols, cols)
}

func (p *relationalProps) addEquivColumnSets(colsets []colset) {
	for _, equiv := range colsets {
		p.addEquivColumns(equiv)
	}
}
