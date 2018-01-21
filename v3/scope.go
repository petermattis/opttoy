package v3

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type scope struct {
	parent *scope
	props  *relationalProps
	state  *queryState
	// Desired number of columns for subqueries found during name resolution and
	// type checking. This only applies to the top-level subqueries that are
	// anchored directly to a relational expression.
	columns int
}

func (s *scope) push(props *relationalProps) *scope {
	return &scope{
		parent: s,
		props:  props,
		state:  s.state,
	}
}

func (s *scope) resolve(expr tree.Expr, desired types.T) tree.TypedExpr {
	// TODO(peter): The caller should specify the desired number of columns. This
	// is needed when a subquery is used by an UPDATE statement.
	s.columns = 1

	expr, _ = tree.WalkExpr(s, expr)
	texpr, err := tree.TypeCheck(expr, &s.state.semaCtx, desired)
	if err != nil {
		panic(err)
	}

	nexpr, err := s.state.evalCtx.NormalizeExpr(texpr)
	if err != nil {
		panic(err)
	}
	return nexpr
}

// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		tblName := tableName(t.TableName.Table())
		colName := columnName(t.ColumnName)

		for ; s != nil; s = s.parent {
			for i := range s.props.columns {
				col := &s.props.columns[i]
				if col.hasColumn(tblName, colName) {
					if tblName == "" && col.table != "" {
						t.TableName.TableName = tree.Name(col.table)
						t.TableName.DBNameOriginallyOmitted = true
					}
					return false, col
				}
			}
		}
		fatalf("unknown column %s", t)

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(s.state.semaCtx.SearchPath)
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
				if _, err := e.TypeCheck(&s.state.semaCtx, types.Any); err != nil {
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

	case *tree.Subquery:
		if t.Exists {
			expr = s.replaceSubquery(t, true /* multi-row */, -1 /* desired-columns */)
		} else {
			expr = s.replaceSubquery(t, false /* multi-row */, s.columns /* desired-columns */)
		}
	}

	// Reset the desired number of columns since if the subquery is a child of
	// any other expression, type checking will verify the number of columns.
	s.columns = -1
	return true, expr
}

func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// Replace a raw subquery node with a typed subquery. multiRow specifies
// whether the subquery is occurring in a single-row or multi-row
// context. desiredColumns specifies the desired number of columns for the
// subquery. Specifying -1 for desiredColumns allows the subquery to return any
// number of columns and is used when the normal type checking machinery will
// verify that the correct number of columns is returned.
func (s *scope) replaceSubquery(sub *tree.Subquery, multiRow bool, desiredColumns int) *subquery {
	result := build(sub.Select, s)
	if desiredColumns > 0 && len(result.props.columns) != desiredColumns {
		n := len(result.props.columns)
		switch desiredColumns {
		case 1:
			panic(fmt.Errorf("subquery must return one column, found %d", n))
		default:
			panic(fmt.Errorf("subquery must return %d columns, found %d", desiredColumns, n))
		}
	}

	subOut := &subquery{
		multiRow: multiRow,
		expr:     result,
		exists:   sub.Exists,
	}

	if sub.Exists {
		subOut.typ = types.Bool
	}

	return subOut
}

type subquery struct {
	typ types.T
	// Is the subquery in a multi-row or single-row context?
	multiRow bool
	expr     *expr
	exists   bool
}

var _ tree.TypedExpr = &subquery{}
var _ tree.VariableExpr = &subquery{}

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

	if len(s.expr.props.columns) == 1 {
		s.typ = s.expr.props.columns[0].typ
	} else {
		t := make(types.TTuple, len(s.expr.props.columns))
		for i := range s.expr.props.columns {
			t[i] = s.expr.props.columns[i].typ
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

// Variable implements the tree.VariableExpr interface. This prevents the
// subquery from being evaluated during normalization.
func (*subquery) Variable() {}

// Eval implements the tree.TypedExpr interface.
func (s *subquery) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("subquery must be replaced before evaluation"))
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
