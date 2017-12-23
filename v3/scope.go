package v3

import (
	"bytes"
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
	return texpr
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

	if s.table {
		// The subquery is in a "table" context. For example:
		//
		//   SELECT 1 IN (SELECT * FROM t)
		t := &types.TTable{
			Cols:   make(types.TTuple, len(s.expr.props.columns)),
			Labels: make([]string, len(s.expr.props.columns)),
		}
		for i := range s.expr.props.columns {
			t.Cols[i] = s.expr.props.columns[i].typ
			t.Labels[i] = string(s.expr.props.columns[i].name)
		}
		// TODO(peter): This should be `s.typ = t`, but doing that causes the query
		// `SELECT 1 IN (SELECT 1)` to fail with:
		//
		//   unsupported comparison operator: <int> IN <setof tuple{int}>
		s.typ = t.Cols
		return s, nil
	}

	// The subquery is in a scalar context. For example:
	//
	//   SELECT (1, 2) = (SELECT 1, 2)
	//
	// If the subquery has a single column
	// we use that as our column type. Otherwise, create a tuple type.
	if len(s.expr.props.columns) == 1 {
		s.typ = s.expr.props.columns[0].typ
	} else {
		t := make(types.TTuple, len(s.expr.props.columns))
		for i := range s.expr.props.columns {
			t[i] = s.expr.props.columns[i].typ
		}
		s.typ = t
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
