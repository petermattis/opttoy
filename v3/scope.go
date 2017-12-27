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
}

func (s *scope) push(props *relationalProps) *scope {
	return &scope{
		parent: s,
		props:  props,
		state:  s.state,
	}
}

func (s *scope) resolve(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr, _ = tree.WalkExpr(s, expr)
	texpr, err := tree.TypeCheck(expr, &s.state.semaCtx, desired)
	if err != nil {
		panic(err)
	}

	// A subquery is only allowed to return a single column in top-level scalar
	// contexts such as projections. Note that the type of the subquery may be a
	// tuple (e.g. `SELECT (1, 2)`) even though the subquery itself returns only
	// a single column.
	//
	// TODO(peter): this seems hacky.
	if sub, ok := texpr.(*subquery); ok {
		if n := len(sub.expr.props.columns); n != 1 {
			panic(fmt.Errorf("subquery must return one column, found %d", n))
		}
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

// NB: This code is adapted from sql/select_name_resolution.go.
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

	case *tree.Subquery:
		return false, &subquery{
			table: false,
			expr:  build(t.Select, s),
		}

	case *tree.ArrayFlatten:
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			t.Subquery = &subquery{
				table: true,
				expr:  build(sub.Select, s),
			}
		}

	case *tree.ComparisonExpr:
		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				t.Right = &subquery{
					table: true,
					expr:  build(sub.Select, s),
				}
			}
		}
	}

	return true, expr
}

func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

type subquery struct {
	typ types.T
	// Is the subquery in a table context or a scalar context.
	table bool
	expr  *expr
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
		if !s.table {
			// The subquery has only a single column and is in a scalar context, we
			// don't want to wrap the type in a tuple. For example:
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
			// The subquery has only a single column and is in a table context. We
			// only wrap if the type of the result column is not a tuple. For
			// example:
			//
			//   SELECT 1 IN (SELECT 1)
			//
			// The type of the subquery will be "vtuple{int}". Now consider:
			//
			//   SELECT (1, 2) IN (SELECT (1, 2))
			//
			// We want the type of subquery to be "vtuple{tuple{tuple{int,int}}}" in
			// order to distinguish it from:
			//
			//   SELECT (1, 2) IN (SELECT 1, 2)
			//
			// Note that this query is semantically valid (the subquery has type
			// "vtuple{tuple{int,int}}"), while the previous query was not.
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

	if s.table {
		// The subquery is in a "table" context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		//
		// Wrap the type in a vtuple.
		s.typ = &types.TVarTuple{
			Typ: s.typ,
		}
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
