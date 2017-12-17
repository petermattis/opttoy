package v3

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// queryState holds per-query state
type queryState struct {
	// map from table name to table
	catalog map[tableName]*table
	// map from table name to the column index for the table's columns within the
	// query (they form a contiguous group starting at this index).
	//
	// TODO(peter): This is used to lookup tables for foreign keys, but such
	// lookups need to be scoped. Or the handling of foreign keys needs to be
	// rethought.
	tables map[tableName]bitmapIndex
	// The set of all columns used by the query.
	columns []columnProps
	semaCtx tree.SemaContext
}

func (q *queryState) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	unimplemented("queryState.IndexedVarEval")
	return nil, fmt.Errorf("unimplemented")
}

func (q *queryState) IndexedVarResolvedType(idx int) types.T {
	return q.columns[idx].typ
}

func (q *queryState) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	unimplemented("queryState.IndexedVarNodeFormatter")
	return nil
}

type columnProps struct {
	name  columnName
	table tableName
	typ   types.T
	index bitmapIndex
	// TODO(peter): Pull hidden out into a bitmap in relationalProps. That will
	// allow changing relationalProps.columns to be a []*columnProps and sharing
	// the columnProps between different relational expressions which differ in
	// whether a column is hidden or not.
	hidden bool
}

func (c columnProps) String() string {
	if c.table == "" {
		return tree.Name(c.name).String()
	}
	return fmt.Sprintf("%s.%s", tree.Name(c.table), tree.Name(c.name))
}

func (c columnProps) hasColumn(tblName tableName, colName columnName) bool {
	if colName != c.name {
		return false
	}
	if tblName == "" {
		return true
	}
	return c.table == tblName
}

func (c columnProps) newVariableExpr(table tableName) *expr {
	if table != "" {
		c.table = table
	}
	e := newVariableExpr(c, c.index)
	e.scalarProps.typ = c.typ
	return e
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
	p.outputCols = p.availableOutputCols()
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

func (p *relationalProps) availableOutputCols() bitmap {
	var v bitmap
	for _, col := range p.columns {
		v.Add(col.index)
	}
	return v
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
