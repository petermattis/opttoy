package build

import (
	"strings"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/petermattis/opttoy/v4/cat"
	"github.com/petermattis/opttoy/v4/opt"
)

type groupby struct {
	// groupingsScope refers to another scope that groups columns in this
	// scope. Any aggregate functions which contain column references to this
	// scope trigger the creation of new grouping columns in the grouping
	// scope. In addition, if an aggregate function contains no column
	// references, then the aggregate will be added to the "nearest" grouping
	// scope:
	//   SELECT MAX(1) FROM t1
	groupingsScope *scope

	// aggs contains all aggregation expressions that were extracted from the
	// query and which will become columns in this scope.
	aggs []opt.GroupID

	// inAgg is true within the body of an aggregate function. inAgg is used
	// to ensure that nested aggregates are disallowed.
	inAgg bool

	// refScope is the scope to which all column references contained by the
	// aggregate function must refer. This is used to detect illegal cases
	// where the aggregate contains column references that point to
	// different scopes:
	//   SELECT a
	//   FROM t1
	//   GROUP BY a
	//   HAVING EXISTS
	//   (
	//     SELECT MAX(t1.a+t2.b)
	//     FROM t2
	//   )
	refScope *scope
}

type scope struct {
	builder  *Builder
	parent   *scope
	cols     []columnProps
	ordering opt.Ordering
	groupby  groupby

	// Desired number of columns for subqueries found during name resolution and
	// type checking. This only applies to the top-level subqueries that are
	// anchored directly to a relational expression.
	columns int
}

func (s *scope) push() *scope {
	return &scope{builder: s.builder, parent: s}
}

func (s *scope) appendColumns(src *scope) {
	s.cols = append(s.cols, src.cols...)
}

func (s *scope) resolveColumnName(tblName cat.TableName, colName cat.ColumnName) *columnProps {
	for curr := s; curr != nil; curr = curr.parent {
		for i := range curr.cols {
			col := &curr.cols[i]
			if col.matches(tblName, colName) {
				return col
			}
		}
	}

	fatalf("unknown column %s", columnProps{name: colName, table: tblName})
	return nil
}

func (s *scope) resolveColumnIndex(index opt.ColumnIndex) *columnProps {
	var aggScope *scope
	for curr := s; curr != nil; curr = curr.parent {
		// Remember whether column reference occurs within an aggregate
		// function.
		if curr.groupby.inAgg {
			aggScope = curr
		}

		for i := range curr.cols {
			col := &curr.cols[i]
			if col.index == index {
				if aggScope != nil {
					if aggScope.groupby.groupingsScope == nil {
						panic("aggregate is not allowed in this context")
					}

					// Ensure that all column references within the same
					// aggregate function refer to columns in the same scope.
					if curr.groupby.refScope == nil {
						curr.groupby.refScope = curr
					} else if curr.groupby.refScope != curr {
						panic("multiple column references within same aggregate must refer to same scope")
					}
				}

				return col
			}
		}
	}

	fatalf("unknown column index %s", index)
	return nil
}

func (s *scope) findAggregate(agg opt.GroupID) *columnProps {
	for i, a := range s.groupby.aggs {
		if a == agg {
			// Aggregate already exists, so return information about the
			// existing column that computes it. Aggregates are always
			// clustered at the end of the column list, in corresponding
			// order.
			return &s.cols[len(s.cols)-len(s.groupby.aggs)+i]
		}
	}

	return nil
}

func (s *scope) startAggFunc() {
	// Disallow nested aggregates and ensure that we're in a grouping scope.
	var found bool
	for curr := s; curr != nil; curr = curr.parent {
		if curr.groupby.inAgg {
			panic("aggregate function cannot be nested within another aggregate function")
		}

		if curr.groupby.groupingsScope != nil {
			found = true
			break
		}
	}

	if !found {
		panic("aggregate function is not allowed in this context")
	}

	s.groupby.inAgg = true
}

func (s *scope) endAggFunc(agg opt.GroupID) (refScope *scope) {
	if !s.groupby.inAgg {
		panic("mismatched calls to start/end aggFunc")
	}

	refScope = s.groupby.refScope
	if refScope == nil {
		// Add the aggregate to the innermost groupings scope since there are
		// no column references contained by this aggregate function.
		for curr := s; curr != nil; curr = curr.parent {
			if curr.groupby.groupingsScope != nil {
				refScope = curr.groupby.groupingsScope
				break
			}
		}
	}

	if refScope == nil {
		panic("not in grouping scope")
	}

	s.groupby.inAgg = false
	return
}

func (s *scope) resolveType(expr tree.Expr, desired types.T) tree.TypedExpr {
	// TODO(peter): The caller should specify the desired number of columns. This
	// is needed when a subquery is used by an UPDATE statement.
	// TODO(andy): shouldn't this be part of the desired type rather than yet
	// another parameter?
	s.columns = 1

	expr, _ = tree.WalkExpr(s, expr)
	texpr, err := tree.TypeCheck(expr, &s.builder.semaCtx, desired)
	if err != nil {
		panic(err)
	}

	return texpr
}

// NB: This code is adapted from sql/select_name_resolution.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		tblName := cat.TableName(t.TableName.Table())
		colName := cat.ColumnName(t.ColumnName)

		for curr := s; curr != nil; curr = curr.parent {
			for i := range curr.cols {
				col := &curr.cols[i]
				if col.matches(tblName, colName) {
					if tblName == "" && col.table != "" {
						// TODO(andy): why is this necessary??
						t.TableName.TableName = tree.Name(col.table)
						t.TableName.DBNameOriginallyOmitted = true
					}
					return false, col
				}
			}
		}

		fatalf("unknown column %s", columnProps{name: colName, table: tblName})
		return false, nil

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(s.builder.semaCtx.SearchPath)
		if err != nil {
			fatalf("%v", err)
		}
		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(tree.VarName)
		if !ok {
			break
		}
		vn, err = vn.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		t.Exprs[0] = vn

		if strings.EqualFold(def.Name, "count") && t.Type == 0 {
			if _, ok := vn.(tree.UnqualifiedStar); ok {
				// Special case handling for COUNT(*). This is a special construct to
				// count the number of rows; in this case * does NOT refer to a set of
				// columns. A * is invalid elsewhere (and will be caught by TypeCheck()).
				// Replace the function with COUNT_ROWS (which doesn't take any
				// arguments).
				cr := tree.Name("COUNT_ROWS")
				e := &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: &tree.UnresolvedName{&cr},
					},
				}
				// We call TypeCheck to fill in FuncExpr internals. This is a fixed
				// expression; we should not hit an error here.
				if _, err := e.TypeCheck(&s.builder.semaCtx, types.Any); err != nil {
					panic(err)
				}
				e.Filter = t.Filter
				e.WindowDef = t.WindowDef
				return true, e
			}
		}

	case *tree.ArrayFlatten:
		// TODO(peter): the ARRAY flatten operator requires a single column from
		// the subquery.
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			t.Subquery = s.replaceSubquery(sub, true /* multi-row */, 1 /* desired-columns */)
		}

	case *tree.ComparisonExpr:
		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				t.Right = s.replaceSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
			}
		}

	case *tree.ExistsExpr:
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			t.Subquery = s.replaceSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
		}

	case *tree.Subquery:
		expr = s.replaceSubquery(t, false /* multi-row */, s.columns /* desired-columns */)
	}

	// Reset the desired number of columns since if the subquery is a child of
	// any other expression, type checking will verify the number of columns.
	s.columns = -1
	return true, expr
}

func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

func (s *scope) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	unimplemented("queryState.IndexedVarEval")
	return nil, fmt.Errorf("unimplemented")
}

func (s *scope) IndexedVarResolvedType(idx int) types.T {
	for curr := s; curr != nil; curr = curr.parent {
		for i := range s.cols {
			col := &curr.cols[i]
			if int(col.index) == idx {
				return col.typ
			}
		}
	}

	fatalf("unknown column index %s", idx)
	return nil
}

func (s *scope) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	unimplemented("queryState.IndexedVarNodeFormatter")
	return nil
}

// Replace a raw subquery node with a typed subquery. multiRow specifies
// whether the subquery is occurring in a single-row or multi-row
// context. desiredColumns specifies the desired number of columns for the
// subquery. Specifying -1 for desiredColumns allows the subquery to return any
// number of columns and is used when the normal type checking machinery will
// verify that the correct number of columns is returned.
func (s *scope) replaceSubquery(sub *tree.Subquery, multiRow bool, desiredColumns int) *subquery {
	out, outScope := s.builder.buildStmt(sub.Select, s)
	if desiredColumns > 0 && len(outScope.cols) != desiredColumns {
		n := len(outScope.cols)
		switch desiredColumns {
		case 1:
			panic(fmt.Errorf("subquery must return one column, found %d", n))
		default:
			panic(fmt.Errorf("subquery must return %d columns, found %d", desiredColumns, n))
		}
	}

	return &subquery{
		cols:     outScope.cols,
		out:      out,
		multiRow: multiRow,
	}
}

type subquery struct {
	cols []columnProps
	out  opt.GroupID

	// Is the subquery in a multi-row or single-row context?
	multiRow bool

	// typ is the lazily resolved type of the subquery.
	typ types.T
}

// String implements the tree.Expr interface.
func (s *subquery) String() string {
	return "subquery.String: unimplemented"
}

// Format implements the tree.Expr interface.
func (s *subquery) Format(ctx *tree.FmtCtx) {
}

// Walk implements the tree.Expr interface.
func (s *subquery) Walk(v tree.Visitor) tree.Expr {
	return s
}

// TypeCheck implements the tree.Expr interface.
func (s *subquery) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	if s.typ != nil {
		return s, nil
	}

	// The typing for subqueries is complex, but regular.
	//
	// * If the subquery is used in a single-row context:
	//
	//   - If the subquery returns a single column with type "U", the type of the
	//     subquery is the type of the column "U". For example:
	//
	//       SELECT 1 = (SELECT 1)
	//
	//     The type of the subquery is "int".
	//
	//   - If the subquery returns multiple columns, the type of the subquery is
	//     "tuple{C}" where "C" expands to all of the types of the columns of the
	//     subquery. For example:
	//
	//       SELECT (1, 'a') = (SELECT 1, 'a')
	//
	//     The type of the subquery is "tuple{int,string}"
	//
	// * If the subquery is used in a multi-row context:
	//
	//   - If the subquery returns a single column with type "U", the type of the
	//     subquery is the singleton tuple of type "U": "tuple{U}". For example:
	//
	//       SELECT 1 IN (SELECT 1)
	//
	//     The type of the subquery's columns is "int" and the type of the
	//     subquery is "tuple{int}".
	//
	//   - If the subquery returns multiple columns, the type of the subquery is
	//     "tuple{tuple{C}}" where "C expands to all of the types of the columns
	//     of the subquery. For example:
	//
	//       SELECT (1, 'a') IN (SELECT 1, 'a')
	//
	//     The types of the subquery's columns are "int" and "string". These are
	//     wrapped into "tuple{int,string}" to form the row type. And these are
	//     wrapped again to form the subquery type "tuple{tuple{int,string}}".
	//
	// Note that these rules produce a somewhat surprising equivalence:
	//
	//   SELECT (SELECT 1, 2) = (SELECT (1, 2))
	//
	// A subquery which returns a single column tuple is equivalent to a subquery
	// which returns the elements of the tuple as individual columns. While
	// surprising, this is necessary for regularity and in order to handle:
	//
	//   SELECT 1 IN (SELECT 1)
	//
	// Without that auto-unwrapping of single-column subqueries, this query would
	// type check as "<int> IN <tuple{tuple{int}}>" which would fail.

	if len(s.cols) == 1 {
		s.typ = s.cols[0].typ
	} else {
		t := make(types.TTuple, len(s.cols))
		for i := range s.cols {
			t[i] = s.cols[i].typ
		}
		s.typ = t
	}

	if s.multiRow {
		// The subquery is in a multi-row context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		//
		// Wrap the type in a tuple.
		s.typ = types.TTuple{s.typ}
	} else {
		// The subquery is in a single-row context. For example:
		//
		//   SELECT (1, 2) = (SELECT 1, 2)
		//
		// Nothing more to do here, the type computed above is sufficient.
	}

	return s, nil
}

// ResolvedType implements the tree.TypedExpr interface.
func (s *subquery) ResolvedType() types.T {
	return s.typ
}

// Eval implements the tree.TypedExpr interface.
func (s *subquery) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("subquery must be replaced before evaluation")
}

// Format implements the tree.Expr interface.
func (c *columnProps) Format(ctx *tree.FmtCtx) {
	ctx.Printf("@%d", c.index+1)
}

// Walk implements the tree.Expr interface.
func (c *columnProps) Walk(v tree.Visitor) tree.Expr {
	return c
}

func (c *columnProps) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return c, nil
}

// ResolvedType implements the tree.TypedExpr interface.
func (c *columnProps) ResolvedType() types.T {
	return c.typ
}

// Variable implements the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*columnProps) Variable() {}

// Eval implements the tree.TypedExpr interface.
func (*columnProps) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("columnProps must be replaced before evaluation"))
}
