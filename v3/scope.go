package v3

import (
	"bytes"
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

func (s *scope) newVariableExpr(idx int) *expr {
	for ; s != nil; s = s.parent {
		col := s.props.findColumnByIndex(idx)
		if col != nil {
			return col.newVariableExpr("")
		}
	}
	return nil
}

// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.IndexedVar:
		return false, t

	case tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		tblName := tableName(t.TableName.Table())
		colName := columnName(t.ColumnName)

		for ; s != nil; s = s.parent {
			for _, col := range s.props.columns {
				if col.hasColumn(tblName, colName) {
					if tblName == "" && col.table != "" {
						t.TableName.TableName = tree.Name(col.table)
						t.TableName.DBNameOriginallyOmitted = true
					}
					return false, tree.NewIndexedVar(col.index)
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
				e := &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.UnresolvedName{tree.Name("COUNT_ROWS")},
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

	return &subquery{
		multiRow: multiRow,
		expr:     result,
	}
}

type subquery struct {
	typ types.T
	// Is the subquery in a multi-row or single-row context?
	multiRow bool
	expr     *expr
}

// String implements the tree.Expr interface.
func (s *subquery) String() string {
	return "subquery.String: unimplemented"
}

// Format implements the tree.Expr interface.
func (s *subquery) Format(buf *bytes.Buffer, f tree.FmtFlags) {
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

	wrap := true
	if len(s.expr.props.columns) == 1 {
		if !s.multiRow {
			// The subquery has only a single column and is in a single-row context,
			// we don't want to wrap the type in a tuple. For example:
			//
			//   SELECT (SELECT 1)
			//
			// and
			//
			//   SELECT (SELECT (1, 2))
			//
			// This will result in the types "int" and "tuple{int,int}" respectively.
			wrap = false
		} else if !types.FamTuple.FamilyEqual(s.expr.props.columns[0].typ) {
			// The subquery has only a single column and is in a multi-row
			// context. We only wrap if the type of the result column is not a
			// tuple. For example:
			//
			//   SELECT 1 IN (SELECT 1)
			//
			// The type of the subquery will be "tuple{int}". Now consider the
			// semantically invalid query:
			//
			//   SELECT (1, 2) IN (SELECT (1, 2))
			//
			// We want the type of subquery to be "tuple{tuple{tuple{int,int}}}" in
			// order to distinguish it from the semantically valid:
			//
			//   SELECT (1, 2) IN (SELECT 1, 2)
			//
			// In this query, the subquery has the type "tuple{tuple{int,int}}"
			// making the IN expression valid.
			//
			// Lastly, note that for the multi-row case, there may be multiple
			// rows. For example:
			//
			//   SELECT 1 IN (VALUES (1), (2), (3))
			//
			// The subquery returns 3 rows and the type is "tuple{int}".
			wrap = false
		}
	}

	if !wrap {
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
		// The subquery is in a scalar context. For example:
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
