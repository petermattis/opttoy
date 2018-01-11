package opt

type logicalPropsID uint32

type LogicalProps struct {
	// UnboundCols is the set of columns that are referenced by some child
	// of the expression, but are not bound within the scope of the
	// expression. For example, a correlated existential subquery would
	// have unbound references to columns outside the scope of the EXISTS.
	// And a join filter expression would have unbound references to
	// columns in the join tables.
	UnboundCols ColSet

	Relational struct {
		// OutputCols is the set of columns that can be projected by the
		// expression. A subset of these columns may actually be required by
		// the parent expression (see PhysicalProps.RequiredCols). The
		// OutputCols set is empty for non-relational expressions.
		OutputCols ColSet

		// NotNullCols is the subset of output columns which cannot be NULL.
		// The NULL-ability of columns flows from the inputs and can also be
		// derived from filters that are NULL-intolerant. The NotNullCols set
		// is empty for non-relational expressions.
		NotNullCols ColSet

		// WeakKeys are the column sets which form weak keys and are subsets of
		// the expression's output columns. WeakKeys returns the empty slice
		// for non-relational expressions.
		//
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
		// weak key is a key if "(WeakKeys[i] & NotNullColumns) == WeakKeys[i]".
		WeakKeys ColSets

		// ForeignKeys are the pairs of column sets which associate foreign key
		// columns in the expression to corresponding unique key columns in the
		// expression. ForeignKeys returns the empty slice for non-relational
		// expressions.
		//
		// A foreign key is a set of columns in the source table that uniquely
		// identify a single row in the destination table. A foreign key thus
		// refers to a primary key or unique key in the destination table. If
		// the source columns are NOT NULL a foreign key can prove the
		// existence of a row in the destination table and can also be used to
		// infer the cardinality of joins when joining on the foreign key.
		// Consider the schema:
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
		// The foreign key constraint specifies that employees.dept_id must
		// match a value in departments.dept_id or be NULL. Because
		// departments.dept_id is NOT NULL (due to being part of the primary
		// key), we know the only rows from employees that will not be in
		// the join are those with a NULL dept_id. So we can transform the
		// query into:
		//
		//   SELECT e.name, e.salary
		//   FROM employees e
		//   WHERE e.dept_id IS NOT NULL
		ForeignKeys []ForeignKeyProps

		// EquivCols are the column sets which form equivalency groups. Each
		// set contains at least 2 columns that will always have the same value
		// in the result set. No column may appear in more than one entry.
		// EquivCols returns the empty slice for non-relational expressions.
		EquivCols ColSets
	}
}

func (p *LogicalProps) addEquivColumns(cols ColSet) {
	for i, equiv := range p.Relational.EquivCols {
		if equiv.Intersects(cols) {
			p.Relational.EquivCols[i].UnionWith(cols)
			return
		}
	}

	p.Relational.EquivCols = append(p.Relational.EquivCols, cols)
}

func (p *LogicalProps) addEquivColumnSets(colsets []ColSet) {
	for _, equiv := range colsets {
		p.addEquivColumns(equiv)
	}
}

type ForeignKeyProps struct {
	src  ColSet
	dest ColSet
}
