package build

import (
	"strings"

	"bytes"
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
	case *tree.AllColumnsSelector:
		tableName := cat.TableName(t.TableName.Table())
		var projections []tree.Expr
		for _, col := range s.cols {
			if col.table == tableName && !col.hidden {
				projections = append(projections, tree.NewIndexedVar(int(col.index)))
			}
		}

		if len(projections) == 0 {
			fatalf("unknown table %s", t)
		}

		return false, &tree.Tuple{Exprs: projections}

	case *tree.IndexedVar:
		return false, t

	case tree.UnresolvedName:
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

					return false, tree.NewIndexedVar(int(col.index))
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
				e := &tree.FuncExpr{
					Func: tree.ResolvableFunctionReference{
						FunctionReference: tree.UnresolvedName{tree.Name("COUNT_ROWS")},
					},
				}
				// We call TypeCheck to fill in FuncExpr internals. This is a fixed
				// expression; we should not hit an error here.
				if _, err := e.TypeCheck(&tree.SemaContext{}, types.Any); err != nil {
					panic(err)
				}
				e.Filter = t.Filter
				e.WindowDef = t.WindowDef
				return true, e
			}
		}

	case *tree.Subquery:
		out, outScope := s.builder.buildStmt(t.Select, s)

		// TODO(peter): We're assuming the type of the subquery is the type of the
		// first column. This is all sorts of wrong.
		return false, &subquery{typ: outScope.cols[0].typ, out: out}
	}

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

type subquery struct {
	typ types.T
	out opt.GroupID
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
