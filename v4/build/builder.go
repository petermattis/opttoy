package build

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/petermattis/opttoy/v4/cat"
	"github.com/petermattis/opttoy/v4/opt"
)

type unaryFactoryFunc func(f *opt.Factory, input opt.GroupID) opt.GroupID
type binaryFactoryFunc func(f *opt.Factory, left, right opt.GroupID) opt.GroupID

var comparisonOpMap = [...]binaryFactoryFunc{
	tree.EQ:                (*opt.Factory).ConstructEq,
	tree.LT:                (*opt.Factory).ConstructLt,
	tree.GT:                (*opt.Factory).ConstructGt,
	tree.LE:                (*opt.Factory).ConstructLe,
	tree.GE:                (*opt.Factory).ConstructGe,
	tree.NE:                (*opt.Factory).ConstructNe,
	tree.In:                (*opt.Factory).ConstructIn,
	tree.NotIn:             (*opt.Factory).ConstructNotIn,
	tree.Like:              (*opt.Factory).ConstructLike,
	tree.NotLike:           (*opt.Factory).ConstructNotLike,
	tree.ILike:             (*opt.Factory).ConstructILike,
	tree.NotILike:          (*opt.Factory).ConstructNotILike,
	tree.SimilarTo:         (*opt.Factory).ConstructSimilarTo,
	tree.NotSimilarTo:      (*opt.Factory).ConstructNotSimilarTo,
	tree.RegMatch:          (*opt.Factory).ConstructRegMatch,
	tree.NotRegMatch:       (*opt.Factory).ConstructNotRegMatch,
	tree.RegIMatch:         (*opt.Factory).ConstructRegIMatch,
	tree.NotRegIMatch:      (*opt.Factory).ConstructNotRegIMatch,
	tree.IsDistinctFrom:    (*opt.Factory).ConstructIsDistinctFrom,
	tree.IsNotDistinctFrom: (*opt.Factory).ConstructIsNotDistinctFrom,
	tree.Is:                (*opt.Factory).ConstructIs,
	tree.IsNot:             (*opt.Factory).ConstructIsNot,
	tree.Any:               (*opt.Factory).ConstructAny,
	tree.Some:              (*opt.Factory).ConstructSome,
	tree.All:               (*opt.Factory).ConstructAll,
}

var binaryOpMap = [...]binaryFactoryFunc{
	tree.Bitand:   (*opt.Factory).ConstructBitand,
	tree.Bitor:    (*opt.Factory).ConstructBitor,
	tree.Bitxor:   (*opt.Factory).ConstructBitxor,
	tree.Plus:     (*opt.Factory).ConstructPlus,
	tree.Minus:    (*opt.Factory).ConstructMinus,
	tree.Mult:     (*opt.Factory).ConstructMult,
	tree.Div:      (*opt.Factory).ConstructDiv,
	tree.FloorDiv: (*opt.Factory).ConstructFloorDiv,
	tree.Mod:      (*opt.Factory).ConstructMod,
	tree.Pow:      (*opt.Factory).ConstructPow,
	tree.Concat:   (*opt.Factory).ConstructConcat,
	tree.LShift:   (*opt.Factory).ConstructLShift,
	tree.RShift:   (*opt.Factory).ConstructRShift,
}

var unaryOpMap = [...]unaryFactoryFunc{
	tree.UnaryPlus:       (*opt.Factory).ConstructUnaryPlus,
	tree.UnaryMinus:      (*opt.Factory).ConstructUnaryMinus,
	tree.UnaryComplement: (*opt.Factory).ConstructUnaryComplement,
}

type Builder struct {
	tree.IndexedVarContainer
	factory *opt.Factory
	stmt    tree.Statement
	semaCtx tree.SemaContext

	// Skip index 0 in order to reserve it to indicate the "unknown" column.
	colMap []columnProps
}

func NewBuilder(factory *opt.Factory, stmt tree.Statement) *Builder {
	b := &Builder{factory: factory, stmt: stmt, colMap: make([]columnProps, 1)}

	ivarHelper := tree.MakeIndexedVarHelper(b, 0)
	b.semaCtx.IVarHelper = &ivarHelper
	b.semaCtx.Placeholders = tree.MakePlaceholderInfo()

	return b
}

func (b *Builder) Build() (root opt.GroupID, required *opt.PhysicalProps) {
	out, outScope := b.buildStmt(b.stmt, &scope{builder: b})

	// Construct the set of physical properties that are required of the root
	// planner group.
	labeledCols := make([]opt.LabeledColumn, len(outScope.cols))
	for i := range outScope.cols {
		col := &outScope.cols[i]
		labeledCols[i] = opt.LabeledColumn{Label: string(col.name), Index: col.index}
	}

	root = out
	required = &opt.PhysicalProps{Ordering: outScope.ordering, Projection: opt.Projection{Columns: labeledCols}}
	return
}

func (b *Builder) buildStmt(stmt tree.Statement, inScope *scope) (out opt.GroupID, outScope *scope) {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return b.buildSelect(stmt, inScope)
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, inScope)
	default:
		unimplemented("%T", stmt)
		return 0, nil
	}
}

func (b *Builder) buildTable(texpr tree.TableExpr, inScope *scope) (out opt.GroupID, outScope *scope) {
	switch source := texpr.(type) {
	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}

		name := cat.TableName(tn.Table())
		tbl := b.factory.Metadata().Catalog().Table(name)

		return b.buildScan(tbl, inScope)

	case *tree.AliasedTableExpr:
		out, outScope = b.buildTable(source.Expr, inScope)

		// Overwrite output properties with any alias information.
		if source.As.Alias != "" {
			if n := len(source.As.Cols); n > 0 && n != len(outScope.cols) {
				fatalf("rename specified %d columns, but table contains %d", n, len(outScope.cols))
			}

			for i := range outScope.cols {
				outScope.cols[i].table = cat.TableName(source.As.Alias)
				if i < len(source.As.Cols) {
					outScope.cols[i].name = cat.ColumnName(source.As.Cols[i])
				}
			}
		}

		return

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, inScope)

	case *tree.JoinTableExpr:
		switch cond := source.Cond.(type) {
		case *tree.OnJoinCond:
			return b.buildOnJoin(source, cond.Expr, inScope)

		case tree.NaturalJoinCond:
			return b.buildNaturalJoin(source, inScope)

		case *tree.UsingJoinCond:
			return b.buildUsingJoin(source, cond.Cols, inScope)

		default:
			unimplemented("%T", source.Cond)
			return 0, nil
		}

	case *tree.Subquery:
		return b.buildStmt(source.Select, inScope)

	default:
		unimplemented("%T", texpr)
		return 0, nil
	}
}

func (b *Builder) buildScan(tbl *cat.Table, inScope *scope) (out opt.GroupID, outScope *scope) {
	tblIndex := b.factory.Metadata().AddTable(tbl)

	outScope = inScope.push()
	for i, col := range tbl.Columns {
		colIndex := b.factory.Metadata().TableColumn(tblIndex, cat.ColumnOrdinal(i))
		col := columnProps{
			index: colIndex,
			name:  col.Name,
			table: tbl.Name,
			typ:   col.Type,
		}

		b.colMap = append(b.colMap, col)
		outScope.cols = append(outScope.cols, col)
	}

	// TODO(peter): the metadata table is used for looking up foreign key
	// references. Currently, this lookup is global, but it likely needs to be
	// scoped.
	return b.factory.ConstructScan(b.factory.InternPrivate(tblIndex)), outScope
}

func (b *Builder) buildOnJoin(
	join *tree.JoinTableExpr,
	on tree.Expr,
	inScope *scope,
) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildTable(join.Left, inScope)
	right, rightScope := b.buildTable(join.Right, inScope)

	// Append columns added by the children, as they are visible to the filter.
	outScope = inScope.push()
	outScope.appendColumns(leftScope)
	outScope.appendColumns(rightScope)

	filter := b.buildScalar(outScope.resolveType(on, types.Bool), outScope)

	return b.constructJoin(join.Join, left, right, filter), outScope
}

func (b *Builder) buildNaturalJoin(join *tree.JoinTableExpr, inScope *scope) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildTable(join.Left, inScope)
	right, rightScope := b.buildTable(join.Right, inScope)

	var common tree.NameList
	for _, leftCol := range leftScope.cols {
		for _, rightCol := range rightScope.cols {
			if leftCol.name == rightCol.name && !leftCol.hidden && !rightCol.hidden {
				common = append(common, tree.Name(leftCol.name))
				break
			}
		}
	}

	filter, outScope := b.buildUsingJoinParts(leftScope.cols, rightScope.cols, common, inScope)

	return b.constructJoin(join.Join, left, right, filter), outScope
}

func (b *Builder) buildUsingJoin(
	join *tree.JoinTableExpr,
	names tree.NameList,
	inScope *scope,
) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildTable(join.Left, inScope)
	right, rightScope := b.buildTable(join.Right, inScope)

	filter, outScope := b.buildUsingJoinParts(leftScope.cols, rightScope.cols, names, inScope)

	return b.constructJoin(join.Join, left, right, filter), outScope
}

func (b *Builder) buildUsingJoinParts(
	leftCols []columnProps,
	rightCols []columnProps,
	names tree.NameList,
	inScope *scope,
) (out opt.GroupID, outScope *scope) {
	joined := make(map[cat.ColumnName]*columnProps, len(names))
	conditions := make([]opt.GroupID, 0, len(names))
	outScope = inScope.push()
	for _, name := range names {
		name := cat.ColumnName(name)

		// For every adjacent pair of tables, add an equality predicate.
		leftCol := findColByName(leftCols, name)
		if leftCol == nil {
			fatalf("unable to resolve name %s", name)
		}

		rightCol := findColByName(rightCols, name)
		if rightCol == nil {
			fatalf("unable to resolve name %s", name)
		}

		outScope.cols = append(outScope.cols, *leftCol)
		joined[name] = &outScope.cols[len(outScope.cols)-1]

		leftVar := b.factory.ConstructVariable(b.factory.InternPrivate(leftCol.index))
		rightVar := b.factory.ConstructVariable(b.factory.InternPrivate(rightCol.index))
		eq := b.factory.ConstructEq(leftVar, rightVar)

		conditions = append(conditions, eq)
	}

	for i, col := range leftCols {
		foundCol, ok := joined[col.name]
		if ok {
			// Hide other columns with the same name.
			if &leftCols[i] == foundCol {
				continue
			}
			col.hidden = true
		}
		outScope.cols = append(outScope.cols, col)
	}

	for _, col := range rightCols {
		_, col.hidden = joined[col.name]
		outScope.cols = append(outScope.cols, col)
	}

	return b.factory.ConstructFilterList(b.factory.StoreList(conditions)), outScope
}

// buildScalarProjection takes the output of buildScalar and adds it as new
// columns to the output scope.
func (b *Builder) buildScalarProjection(scalar tree.TypedExpr, inScope, outScope *scope) opt.GroupID {
	switch t := scalar.(type) {
	case *tree.ParenExpr:
		return b.buildScalarProjection(t.TypedInnerExpr(), inScope, outScope)

	case tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}

		return b.buildScalarProjection(vn, inScope, outScope)

	case *tree.IndexedVar:
		// Directly project columns that are bound to the current scope, but
		// synthesize a new wrapper column for unbound columns (i.e. columns
		// bound to an ancestor scope). This ensures that relational expression
		// inputs always have disjoint output columns. Therefore, different
		// instances of the same column can have different values, even when
		// joined.
		colIndex := opt.ColumnIndex(t.Idx)
		out := b.factory.ConstructVariable(b.factory.InternPrivate(colIndex))

		col := findColByIndex(inScope.cols, colIndex)
		if col == nil {
			// Unbound reference, so project a synthesized wrapper column.
			label := string(b.colMap[colIndex].name)
			b.synthesizeColumn(outScope, label, scalar.ResolvedType())
		} else {
			outScope.cols = append(outScope.cols, b.colMap[colIndex])
		}

		return out

	default:
		out := b.buildScalar(scalar, inScope)
		b.synthesizeColumn(outScope, "", scalar.ResolvedType())
		return out
	}
}

func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) opt.GroupID {
	switch t := scalar.(type) {
	case *tree.Tuple:
		list := make([]opt.GroupID, 0, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope)
		}

		return b.factory.ConstructOrderedList(b.factory.StoreList(list))

	case *tree.ParenExpr:
		return b.buildScalar(t.TypedInnerExpr(), inScope)

	case *tree.AndExpr:
		return b.factory.ConstructAnd(b.buildScalar(t.TypedLeft(), inScope), b.buildScalar(t.TypedRight(), inScope))
	case *tree.OrExpr:
		return b.factory.ConstructOr(b.buildScalar(t.TypedLeft(), inScope), b.buildScalar(t.TypedRight(), inScope))
	case *tree.NotExpr:
		return b.factory.ConstructNot(b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.BinaryExpr:
		return binaryOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope))
	case *tree.ComparisonExpr:
		return comparisonOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope))
	case *tree.UnaryExpr:
		return unaryOpMap[t.Operator](b.factory, b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.IndexedVar:
		return b.factory.ConstructVariable(b.factory.InternPrivate(opt.ColumnIndex(t.Idx)))

	case *tree.Placeholder:
		return b.factory.ConstructPlaceholder(b.factory.InternPrivate(t))

	case tree.Datum:
		return b.factory.ConstructConst(b.factory.InternPrivate(t))

	case *tree.FuncExpr:
		return b.buildFunction(t, inScope)

	case *tree.ExistsExpr:
		out := b.buildScalar(inScope.resolveType(t.Subquery, types.Any), inScope)
		return b.factory.ConstructExists(out)

	case *subquery:
		// TODO(peter): a subquery in a scalar context needs to be wrapped with
		// some sort of scalar expression. For example, `SELECT (SELECT 1)`. The
		// `SELECT 1` subquery is being used as a projection. We need to wrap the
		// relational expression in something like a subqueryOp scalar expression
		// that is typed according to the subquery.
		v := b.factory.ConstructVariable(b.factory.InternPrivate(t.col.index))
		return b.factory.ConstructSubquery(t.out, v)

	default:
		// NB: we can't type assert on tree.dNull because the type is not
		// exported.
		if scalar == tree.DNull {
			return b.factory.ConstructConst(b.factory.InternPrivate(scalar))
		}
	}

	unimplemented("%T", scalar)
	return 0
}

func (b *Builder) buildFunction(f *tree.FuncExpr, inScope *scope) opt.GroupID {
	def, err := f.Func.Resolve(tree.SearchPath{})
	if err != nil {
		fatalf("%v", err)
	}

	isAgg := isAggregate(def)
	if isAgg {
		// Look for any column references contained within the aggregate.
		inScope.startAggFunc()
	}

	argList := make([]opt.GroupID, 0, len(f.Exprs))
	for _, pexpr := range f.Exprs {
		var arg opt.GroupID
		if _, ok := pexpr.(tree.UnqualifiedStar); ok {
			arg = b.factory.ConstructConst(b.factory.InternPrivate(tree.NewDInt(1)))
		} else {
			arg = b.buildScalar(pexpr.(tree.TypedExpr), inScope)
		}

		argList = append(argList, arg)
	}

	function := b.factory.ConstructFunction(b.factory.StoreList(argList), b.factory.InternPrivate(def))

	if isAgg {
		refScope := inScope.endAggFunc(function)

		// If the aggregate already exists as a column, use that. Otherwise
		// create a new column and add it the list of aggregates that need to
		// be computed by the groupby expression.
		col := refScope.findAggregate(function)
		if col == nil {
			col = b.synthesizeColumn(refScope, "", f.ResolvedType())

			// Add the aggregate to the list of aggregates that need to be computed by
			// the groupby expression.
			refScope.groupby.aggs = append(refScope.groupby.aggs, function)
		}

		// Replace the function call with a reference to the column.
		return b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
	}

	return function
}

func (b *Builder) buildSelect(stmt *tree.Select, inScope *scope) (out opt.GroupID, outScope *scope) {
	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		return b.buildSelectClause(stmt, inScope)

	case *tree.UnionClause:
		out, outScope = b.buildUnion(t, inScope)

	case *tree.ParenSelect:
		return b.buildSelect(t.Select, inScope)

	// TODO(peter): case *tree.ValuesClause:

	default:
		unimplemented("%T", stmt.Select)
	}

	// TODO(peter): stmt.Limit

	out, outScope.ordering = b.buildOrderBy(out, stmt.OrderBy, outScope)
	return
}

// Pass the entire Select statement rather than just the select clause in
// order to handle ORDER BY scoping rules. ORDER BY can sort results using
// columns from the FROM/GROUP BY clause and/or from the projection list.
func (b *Builder) buildSelectClause(stmt *tree.Select, inScope *scope) (out opt.GroupID, outScope *scope) {
	sel := stmt.Select.(*tree.SelectClause)

	var fromScope *scope
	out, fromScope = b.buildFrom(sel.From, sel.Where, inScope)

	// The "from" columns are visible to the grouping expressions. Even if
	// there is no group by clause in the expression, buildGroupingList will
	// still create a scope in order to track aggregate expressions in the
	// project list, since these cause a group by to be built.
	groupings, groupingsScope := b.buildGroupingList(sel.GroupBy, fromScope)

	// Any "grouping" columns are visible to both the "having" and "projection"
	// expressions. The build has the side effect of extracting aggregations.
	var having opt.GroupID
	if groupings == nil {
		// No groupby clause, so use "from" scope directly.
		groupingsScope = fromScope
	} else {
		having = b.buildScalar(groupingsScope.resolveType(sel.Having.Expr, types.Bool), groupingsScope)
	}

	// Any grouping columns are visible to the projection expressions. If the
	// projection is empty or a simple pass-through, then buildProjectionList
	// will return nil values.
	projections, projectionsScope := b.buildProjectionList(sel.Exprs, groupingsScope)

	// Wrap with groupby operator if groupings or aggregates exist.
	if groupings != nil || len(groupingsScope.groupby.aggs) > 0 {
		// Any aggregate columns that were discovered would have been appended
		// to the end of the grouping scope.
		aggCols := groupingsScope.cols[len(groupingsScope.cols)-len(groupingsScope.groupby.aggs):]
		aggList := b.constructProjectionList(groupingsScope.groupby.aggs, aggCols)

		var groupingCols []columnProps
		if groupings != nil {
			groupingCols = groupingsScope.cols
		}

		groupingList := b.constructProjectionList(groupings, groupingCols)
		out = b.factory.ConstructGroupBy(out, groupingList, aggList)

		// Wrap with having filter if it exists.
		if having != 0 {
			out = b.factory.ConstructSelect(out, having)
		}
	}

	// Set final output scope.
	if projections != nil {
		outScope = projectionsScope
	} else {
		outScope = groupingsScope
	}

	if stmt.OrderBy != nil {
		// OrderBy can reference columns from either the from/grouping clause
		// or the projections clause, so combine them in a single projection.
		var orderByScope *scope
		out, orderByScope = b.buildAppendingProject(out, groupingsScope, projections, projectionsScope)

		// Wrap with distinct operator if it exists.
		out = b.buildDistinct(out, sel.Distinct, outScope.cols, orderByScope)

		// Build projection containing any additional synthetic order by
		// columns and set the ordering on the output scope.
		out, outScope.ordering = b.buildOrderBy(out, stmt.OrderBy, orderByScope)
		return
	}

	// Wrap with project operator if it exists.
	if projections != nil {
		out = b.factory.ConstructProject(out, b.constructProjectionList(projections, projectionsScope.cols))
	}

	// Wrap with distinct operator if it exists.
	out = b.buildDistinct(out, sel.Distinct, outScope.cols, outScope)
	return
}

func (b *Builder) buildFrom(from *tree.From, where *tree.Where, inScope *scope) (out opt.GroupID, outScope *scope) {
	var left, right opt.GroupID

	for _, table := range from.Tables {
		var rightScope *scope
		right, rightScope = b.buildTable(table, inScope)

		if left == 0 {
			left = right
			outScope = rightScope
			continue
		}

		outScope.appendColumns(rightScope)

		left = b.factory.ConstructInnerJoin(left, right, b.factory.ConstructTrue())
	}

	if left == 0 {
		// TODO(peter): This should be a table with 1 row and 0 columns to match
		// current cockroach behavior.
		out = b.factory.ConstructValues()
		outScope = inScope
	} else {
		out = left
	}

	if where != nil {
		// All "from" columns are visible to the filter expression.
		texpr := outScope.resolveType(where.Expr, types.Bool)
		filter := b.buildScalar(texpr, outScope)
		out = b.factory.ConstructSelect(out, filter)
	}

	return
}

func (b *Builder) buildGroupingList(groupBy tree.GroupBy, inScope *scope) (groupings []opt.GroupID, outScope *scope) {
	// Create a grouping scope even if there is no explicit groupby in the
	// query, since one or more aggregate functions in the projection list
	// triggers an implicit groupby.
	outScope = inScope.push()

	// Set the grouping scope so that any aggregates will be added to the set
	// of grouping columns.
	inScope.groupby.groupingsScope = outScope

	if groupBy == nil {
		return
	}

	groupings = make([]opt.GroupID, 0, len(groupBy))
	for _, e := range groupBy {
		scalar := b.buildScalarProjection(inScope.resolveType(e, types.Any), inScope, outScope)
		groupings = append(groupings, scalar)
	}

	return
}

func (b *Builder) buildProjectionList(
	selects tree.SelectExprs,
	inScope *scope,
) (projections []opt.GroupID, outScope *scope) {
	if len(selects) == 0 {
		return nil, nil
	}

	outScope = inScope.push()
	projections = make([]opt.GroupID, 0, len(selects))
	for _, e := range selects {
		end := len(outScope.cols)
		subset := b.buildProjection(e.Expr, inScope, outScope)
		projections = append(projections, subset...)

		// Update the name of the column if there is an alias defined.
		if e.As != "" {
			for i := range outScope.cols[end:] {
				outScope.cols[i].name = cat.ColumnName(e.As)
			}
		}
	}

	// Don't add an unnecessary "pass through" project expression.
	if len(outScope.cols) == len(inScope.cols) {
		matches := true
		for i := range inScope.cols {
			if inScope.cols[i].index != outScope.cols[i].index {
				matches = false
				break
			}
		}

		if matches {
			return nil, nil
		}
	}

	return
}

func (b *Builder) buildProjection(projection tree.Expr, inScope, outScope *scope) (projections []opt.GroupID) {
	switch t := projection.(type) {
	case tree.UnqualifiedStar:
		for _, col := range inScope.cols {
			if !col.hidden {
				v := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
				projections = append(projections, v)
				outScope.cols = append(outScope.cols, col)
			}
		}

		if len(projections) == 0 {
			fatalf("failed to expand *")
		}

		return

	case *tree.AllColumnsSelector:
		tableName := cat.TableName(t.TableName.Table())
		for _, col := range inScope.cols {
			if col.table == tableName && !col.hidden {
				v := b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
				projections = append(projections, v)
				outScope.cols = append(outScope.cols, col)
			}
		}

		if len(projections) == 0 {
			fatalf("unknown table %s", t)
		}

		return projections

	case tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}

		return b.buildProjection(vn, inScope, outScope)

	default:
		texpr := inScope.resolveType(projection, types.Any)
		return []opt.GroupID{b.buildScalarProjection(texpr, inScope, outScope)}
	}
}

func (b *Builder) buildDistinct(in opt.GroupID, distinct bool, byCols []columnProps, inScope *scope) opt.GroupID {
	if !distinct {
		return in
	}

	// Distinct is equivalent to group by without any aggregations.
	groupings := make([]opt.GroupID, 0, len(byCols))
	for i := range byCols {
		v := b.factory.ConstructVariable(b.factory.InternPrivate(byCols[i].index))
		groupings = append(groupings, v)
	}

	list := b.factory.ConstructOrderedList(b.factory.StoreList(groupings))
	return b.factory.ConstructGroupBy(in, list, 0)
}

func (b *Builder) buildOrderBy(
	in opt.GroupID,
	orderBy tree.OrderBy,
	inScope *scope,
) (out opt.GroupID, ordering opt.Ordering) {
	if orderBy == nil {
		return in, nil
	}

	orderScope := inScope.push()

	projections := make([]opt.GroupID, len(orderBy))
	for _, order := range orderBy {
		scalar := b.buildScalarProjection(inScope.resolveType(order.Expr, types.Any), inScope, orderScope)
		projections = append(projections, scalar)
	}

	out, _ = b.buildAppendingProject(in, inScope, projections, orderScope)

	// Order-by is not a relational expression, but instead a required property
	// on the output. We set the required ordering on the scope so that callers
	// can extract that and pass that as a required physical property to the
	// optimizer.
	ordering = make(opt.Ordering, 0, len(orderBy))
	for i := range orderScope.cols {
		index := orderScope.cols[i].index
		if orderBy[i].Direction == tree.Descending {
			index = -(index + 1)
		}

		ordering = append(ordering, index)
	}

	return
}

func (b *Builder) buildAppendingProject(
	in opt.GroupID,
	inScope *scope,
	projections []opt.GroupID,
	projectionsScope *scope,
) (out opt.GroupID, outScope *scope) {
	if projections == nil {
		return in, inScope
	}

	outScope = projectionsScope.push()

	combined := make([]opt.GroupID, 0, len(inScope.cols)+len(projectionsScope.cols))
	for i := range inScope.cols {
		col := &inScope.cols[i]
		outScope.cols = append(outScope.cols, *col)
		combined = append(combined, b.factory.ConstructVariable(b.factory.InternPrivate(col.index)))
	}

	for i := range projectionsScope.cols {
		col := &projectionsScope.cols[i]

		// Only append projection columns that aren't already present.
		if findColByIndex(outScope.cols, col.index) == nil {
			outScope.cols = append(outScope.cols, *col)
			combined = append(combined, b.factory.ConstructVariable(b.factory.InternPrivate(col.index)))
		}
	}

	if len(outScope.cols) == len(inScope.cols) {
		// All projection columns were already present, so no need to construct
		// the projection expression.
		return in, inScope
	}

	out = b.factory.ConstructProject(in, b.constructProjectionList(combined, outScope.cols))
	return
}

func (b *Builder) buildUnion(clause *tree.UnionClause, inScope *scope) (out opt.GroupID, outScope *scope) {
	left, leftScope := b.buildSelect(clause.Left, inScope)
	right, rightScope := b.buildSelect(clause.Right, inScope)

	// Build map from left columns to right columns.
	colMap := make(opt.ColMap)
	for i := range leftScope.cols {
		colMap[leftScope.cols[i].index] = rightScope.cols[i].index
	}

	switch clause.Type {
	case tree.UnionOp:
		out = b.factory.ConstructUnion(left, right, b.factory.InternPrivate(&colMap))
	case tree.IntersectOp:
		out = b.factory.ConstructIntersect(left, right)
	case tree.ExceptOp:
		out = b.factory.ConstructExcept(left, right)
	}

	outScope = leftScope
	return
}

func (b *Builder) synthesizeColumn(scope *scope, label string, typ types.T) *columnProps {
	if label == "" {
		label = fmt.Sprintf("column%d", len(scope.cols)+1)
	}

	colIndex := b.factory.Metadata().AddColumn(label)
	col := columnProps{typ: typ, index: colIndex}
	b.colMap = append(b.colMap, col)
	scope.cols = append(scope.cols, col)
	return &scope.cols[len(scope.cols)-1]
}

func (b *Builder) constructJoin(joinType string, left, right, filter opt.GroupID) opt.GroupID {
	switch joinType {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		return b.factory.ConstructInnerJoin(left, right, filter)
	case "LEFT JOIN":
		return b.factory.ConstructLeftJoin(left, right, filter)
	case "RIGHT JOIN":
		return b.factory.ConstructRightJoin(left, right, filter)
	case "FULL JOIN":
		return b.factory.ConstructFullJoin(left, right, filter)
	default:
		unimplemented("unsupported JOIN type %s", joinType)
		return 0
	}
}

func (b *Builder) constructProjectionList(items []opt.GroupID, cols []columnProps) opt.GroupID {
	// Create column index list parameter to the ProjectionList op.
	var colSet opt.ColSet
	for i := range cols {
		colSet.Add(int(cols[i].index))
	}

	return b.factory.ConstructProjections(b.factory.StoreList(items), b.factory.InternPrivate(&colSet))
}

func (b *Builder) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	unimplemented("queryState.IndexedVarEval")
	return nil, fmt.Errorf("unimplemented")
}

func (b *Builder) IndexedVarResolvedType(idx int) types.T {
	return b.colMap[opt.ColumnIndex(idx)].typ
}

func (b *Builder) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	unimplemented("queryState.IndexedVarNodeFormatter")
	return nil
}

func isAggregate(def *tree.FunctionDefinition) bool {
	return strings.EqualFold(def.Name, "count") ||
		strings.EqualFold(def.Name, "count_rows") ||
		strings.EqualFold(def.Name, "min") ||
		strings.EqualFold(def.Name, "max") ||
		strings.EqualFold(def.Name, "sum") ||
		strings.EqualFold(def.Name, "avg")
}

func findColByName(cols []columnProps, name cat.ColumnName) *columnProps {
	for i := range cols {
		col := &cols[i]
		if col.name == name {
			return col
		}
	}

	return nil
}

func findColByIndex(cols []columnProps, colIndex opt.ColumnIndex) *columnProps {
	for i := range cols {
		col := &cols[i]
		if col.index == colIndex {
			return col
		}
	}

	return nil
}
