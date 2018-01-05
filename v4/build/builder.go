package build

import (
	"fmt"
	"strings"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
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
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, inScope)

	case *tree.Select:
		return b.buildSelect(stmt, inScope)

		// *tree.AlterSequence
		// *tree.AlterTable
		// *tree.AlterUserSetPassword
		// *tree.Backup
		// *tree.BeginTransaction
		// *tree.CancelJob
		// *tree.CancelQuery
		// *tree.CommitTransaction
		// *tree.CopyFrom
		// *tree.CreateDatabase
		// *tree.CreateIndex
		// *tree.CreateSequence
		// *tree.CreateStats
		// *tree.CreateTable
		// *tree.CreateUser
		// *tree.CreateView
		// *tree.Deallocate
		// *tree.Delete
		// *tree.Discard
		// *tree.DropDatabase
		// *tree.DropIndex
		// *tree.DropSequence
		// *tree.DropTable
		// *tree.DropUser
		// *tree.DropView
		// *tree.Execute
		// *tree.Explain
		// *tree.Grant
		// *tree.Import
		// *tree.Insert
		// *tree.PauseJob
		// *tree.Prepare
		// *tree.ReleaseSavepoint
		// *tree.RenameColumn
		// *tree.RenameDatabase
		// *tree.RenameIndex
		// *tree.RenameTable
		// *tree.Restore
		// *tree.ResumeJob
		// *tree.Revoke
		// *tree.RollbackToSavepoint
		// *tree.RollbackTransaction
		// *tree.Savepoint
		// *tree.Scatter
		// *tree.Scrub
		// *tree.SelectClause
		// *tree.SetClusterSetting
		// *tree.SetSessionCharacteristics
		// *tree.SetTransaction
		// *tree.SetVar
		// *tree.SetZoneConfig
		// *tree.ShowBackup
		// *tree.ShowClusterSetting
		// *tree.ShowColumns
		// *tree.ShowConstraints
		// *tree.ShowCreateTable
		// *tree.ShowCreateView
		// *tree.ShowDatabases
		// *tree.ShowFingerprints
		// *tree.ShowGrants
		// *tree.ShowHistogram
		// *tree.ShowIndex
		// *tree.ShowJobs
		// *tree.ShowQueries
		// *tree.ShowRanges
		// *tree.ShowSessions
		// *tree.ShowTableStats
		// *tree.ShowTables
		// *tree.ShowTrace
		// *tree.ShowTransactionStatus
		// *tree.ShowUsers
		// *tree.ShowVar
		// *tree.ShowZoneConfig
		// *tree.Split
		// *tree.TestingRelocate
		// *tree.Truncate
		// *tree.UnionClause
		// *tree.Update
		// *tree.ValuesClause
		// tree.SelectStatement

	default:
		fatalf("unexpected statement: %T", stmt)
		return 0, nil
	}
}

func (b *Builder) buildTable(texpr tree.TableExpr, inScope *scope) (out opt.GroupID, outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch source := texpr.(type) {
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

	case *tree.FuncExpr:
		unimplemented("%T", texpr)

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

	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}

		name := cat.TableName(tn.Table())
		tbl := b.factory.Metadata().Catalog().Table(name)

		return b.buildScan(tbl, inScope)

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, inScope)

	case *tree.StatementSource:
		unimplemented("%T", texpr)

	case *tree.Subquery:
		return b.buildStmt(source.Select, inScope)

	case *tree.TableRef:
		unimplemented("%T", texpr)

	default:
		fatalf("unexpected table expr: %T", texpr)
	}

	return 0, nil
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

	return b.factory.ConstructFilters(b.factory.StoreList(conditions)), outScope
}

// buildScalarProjection takes the output of buildScalar and adds it as new
// columns to the output scope.
func (b *Builder) buildScalarProjection(texpr tree.TypedExpr, inScope, outScope *scope) opt.GroupID {
	// NB: The case statements are sorted lexicographically.
	switch t := texpr.(type) {
	case *columnProps:
		colIndex := opt.ColumnIndex(t.index)
		out := b.factory.ConstructVariable(b.factory.InternPrivate(colIndex))
		outScope.cols = append(outScope.cols, b.colMap[colIndex])
		return out

	case *tree.FuncExpr:
		out, col := b.buildFunction(t, inScope)
		if col != nil {
			// Function was mapped to a column reference, such as in the case
			// of an aggregate.
			outScope.cols = append(outScope.cols, b.colMap[col.index])
		}
		return out

	case *tree.ParenExpr:
		return b.buildScalarProjection(t.TypedInnerExpr(), inScope, outScope)

	case tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return b.buildScalarProjection(vn, inScope, outScope)

	default:
		out := b.buildScalar(texpr, inScope)
		b.synthesizeColumn(outScope, "", texpr.ResolvedType())
		return out
	}
}

func (b *Builder) buildScalar(scalar tree.TypedExpr, inScope *scope) opt.GroupID {
	switch t := scalar.(type) {
	case *columnProps:
		return b.factory.ConstructVariable(b.factory.InternPrivate(t.index))

	case *tree.AllColumnsSelector:
		fatalf("unexpected unresolved scalar expr: %T", scalar)

	case *tree.AndExpr:
		return b.factory.ConstructAnd(b.buildScalar(t.TypedLeft(), inScope), b.buildScalar(t.TypedRight(), inScope))

	case *tree.Array:
		unimplemented("%T", scalar)

	case *tree.ArrayFlatten:
		unimplemented("%T", scalar)

	case *tree.BinaryExpr:
		return binaryOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope))

	case *tree.CaseExpr:
		unimplemented("%T", scalar)

	case *tree.CastExpr:
		unimplemented("%T", scalar)

	case *tree.CoalesceExpr:
		unimplemented("%T", scalar)

	case *tree.CollateExpr:
		unimplemented("%T", scalar)

	case *tree.ColumnItem:
		fatalf("unexpected unresolved scalar expr: %T", scalar)

	case *tree.ComparisonExpr:
		// TODO(peter): handle t.SubOperator.
		return comparisonOpMap[t.Operator](b.factory,
			b.buildScalar(t.TypedLeft(), inScope),
			b.buildScalar(t.TypedRight(), inScope))

	case tree.DefaultVal:
		unimplemented("%T", scalar)

	case *tree.ExistsExpr:
		// TODO(peter): the decorrelation code currently expects the subquery to be
		// unwrapped for EXISTS expressions.
		subquery := t.Subquery.(*subquery)
		return b.factory.ConstructExists(subquery.out)

	case *tree.FuncExpr:
		out, _ := b.buildFunction(t, inScope)
		return out

	case *tree.IfExpr:
		unimplemented("%T", scalar)

	case *tree.IndirectionExpr:
		unimplemented("%T", scalar)

	case *tree.IsOfTypeExpr:
		unimplemented("%T", scalar)

	case *tree.NotExpr:
		return b.factory.ConstructNot(b.buildScalar(t.TypedInnerExpr(), inScope))

	case *tree.NullIfExpr:
		unimplemented("%T", scalar)

	case *tree.OrExpr:
		return b.factory.ConstructOr(b.buildScalar(t.TypedLeft(), inScope), b.buildScalar(t.TypedRight(), inScope))

	case *tree.ParenExpr:
		return b.buildScalar(t.TypedInnerExpr(), inScope)

	case *tree.Placeholder:
		return b.factory.ConstructPlaceholder(b.factory.InternPrivate(t))

	case *tree.RangeCond:
		unimplemented("%T", scalar)

	case *subquery:
		v := b.factory.ConstructVariable(b.factory.InternPrivate(t.cols[0].index))
		return b.factory.ConstructSubquery(t.out, v)

	case *tree.Tuple:
		list := make([]opt.GroupID, 0, len(t.Exprs))
		for i := range t.Exprs {
			list[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope)
		}

		return b.factory.ConstructOrderedList(b.factory.StoreList(list))

	case *tree.UnaryExpr:
		return unaryOpMap[t.Operator](b.factory, b.buildScalar(t.TypedInnerExpr(), inScope))

	case tree.UnqualifiedStar:
		fatalf("unexpected unresolved scalar expr: %T", scalar)

	case tree.UnresolvedName:
		fatalf("unexpected unresolved scalar expr: %T", scalar)

		// NB: this is the exception to the sorting of the case statements. The
		// tree.Datum case needs to occur after *tree.Placeholder which implements
		// Datum.
	case tree.Datum:
		// *DArray
		// *DBool
		// *DBytes
		// *DCollatedString
		// *DDate
		// *DDecimal
		// *DFloat
		// *DIPAddr
		// *DInt
		// *DInterval
		// *DJSON
		// *DOid
		// *DOidWrapper
		// *DString
		// *DTable
		// *DTime
		// *DTimestamp
		// *DTimestampTZ
		// *DTuple
		// *DUuid
		// dNull
		return b.factory.ConstructConst(b.factory.InternPrivate(t))
	}

	fatalf("unexpected scalar expr: %T", scalar)
	return 0
}

func (b *Builder) buildFunction(f *tree.FuncExpr, inScope *scope) (out opt.GroupID, col *columnProps) {
	def, err := f.Func.Resolve(b.semaCtx.SearchPath)
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

	out = b.factory.ConstructFunction(b.factory.StoreList(argList), b.factory.InternPrivate(def))

	if isAgg {
		refScope := inScope.endAggFunc(out)

		// If the aggregate already exists as a column, use that. Otherwise
		// create a new column and add it the list of aggregates that need to
		// be computed by the groupby expression.
		// TODO(andy): turns out this doesn't really do anything because the
		//             list passed to ConstructFunction isn't interned.
		col = refScope.findAggregate(out)
		if col == nil {
			col = b.synthesizeColumn(refScope, "", f.ResolvedType())

			// Add the aggregate to the list of aggregates that need to be computed by
			// the groupby expression.
			refScope.groupby.aggs = append(refScope.groupby.aggs, out)
		}

		// Replace the function call with a reference to the column.
		out = b.factory.ConstructVariable(b.factory.InternPrivate(col.index))
	}

	return
}

func (b *Builder) buildSelect(stmt *tree.Select, inScope *scope) (out opt.GroupID, outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch t := stmt.Select.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(t.Select, inScope)

	case *tree.SelectClause:
		return b.buildSelectClause(stmt, inScope)

	case *tree.UnionClause:
		out, outScope = b.buildUnion(t, inScope)

	case *tree.ValuesClause:
		return b.buildValuesClause(t, inScope)

	default:
		fatalf("unexpected select statement: %T", stmt.Select)
	}

	out, outScope.ordering = b.buildOrderBy(out, stmt.OrderBy, outScope)
	// TODO(peter): stmt.Limit
	return
}

func (b *Builder) buildValuesClause(values *tree.ValuesClause, inScope *scope) (out opt.GroupID, outScope *scope) {
	// TODO(andy): need to adapt this code
	return 0, nil
	/*	var numCols int
		if len(values.Tuples) > 0 {
			numCols = len(values.Tuples[0].Exprs)
		}

		result = &expr{
			op: valuesOp,
			props: &relationalProps{
				columns: make([]columnProps, numCols),
			},
		}
		for i := range result.props.columns {
			result.props.columns[i].name = columnName(fmt.Sprintf("column%d", i+1))
		}

		buf := make([]*expr, len(t.Tuples)*(numCols+1))
		rows := buf[:0:len(t.Tuples)]
		buf = buf[len(t.Tuples):]

		for _, tuple := range t.Tuples {
			if numCols != len(tuple.Exprs) {
				panic(fmt.Errorf(
					"VALUES lists must all be the same length, expected %d columns, found %d",
					numCols, len(tuple.Exprs)))
			}

			row := buf[:numCols:numCols]
			buf = buf[numCols:]

			for i, expr := range tuple.Exprs {
				row[i] = buildScalar(scope.resolve(expr, types.Any), scope)
				typ := row[i].scalarProps.typ
				if result.props.columns[i].typ == nil || result.props.columns[i].typ == types.Null {
					result.props.columns[i].typ = typ
				} else if typ != types.Null && !typ.Equivalent(result.props.columns[i].typ) {
					panic(fmt.Errorf("VALUES list type mismatch, %s for %s", typ, result.props.columns[i].typ))
				}
			}

			rows = append(rows, &expr{
				op:          tupleOp,
				children:    row,
				scalarProps: &scalarProps{},
			})
		}

		typ := make(types.TTuple, len(result.props.columns))
		for i := range result.props.columns {
			typ[i] = result.props.columns[i].typ
		}
		for i := range rows {
			rows[i].scalarProps.typ = typ
		}

		// TODO(peter): A VALUES clause can contain subqueries and other
		// non-trivial expressions. We probably need to store the tuples in an
		// explicit child of the values node, rather than in private data.
		result.private = &expr{
			op:          orderedListOp,
			children:    rows,
			scalarProps: &scalarProps{},
		}*/
}

// Pass the entire Select statement rather than just the select clause in
// order to handle ORDER BY scoping rules. ORDER BY can sort results using
// columns from the FROM/GROUP BY clause and/or from the projection list.
func (b *Builder) buildSelectClause(stmt *tree.Select, inScope *scope) (out opt.GroupID, outScope *scope) {
	sel := stmt.Select.(*tree.SelectClause)

	var fromScope *scope
	out, fromScope = b.buildFrom(sel.From, sel.Where, inScope)

	// The "from" columns are visible to any grouping expressions.
	groupings, groupingsScope := b.buildGroupingList(sel.GroupBy, fromScope)

	// Set the grouping scope so that any aggregates will be added to the set
	// of grouping columns.
	if groupings == nil {
		// Even though there is no groupby clause, create a grouping scope
		// anyway, since one or more aggregate functions in the projection list
		// triggers an implicit groupby.
		groupingsScope = fromScope.push()
		fromScope.groupby.groupingsScope = groupingsScope
	} else {
		// Add aggregate columns directly to the existing groupings scope.
		groupingsScope.groupby.groupingsScope = groupingsScope
	}

	// Any "grouping" columns are visible to both the "having" and "projection"
	// expressions. The build has the side effect of extracting aggregation
	// columns.
	var having opt.GroupID
	if groupings != nil {
		having = b.buildScalar(groupingsScope.resolveType(sel.Having.Expr, types.Bool), groupingsScope)
	}

	// If the projection is empty or a simple pass-through, then
	// buildProjectionList will return nil values.
	var projections []opt.GroupID
	var projectionsScope *scope
	if groupings == nil {
		projections, projectionsScope = b.buildProjectionList(sel.Exprs, fromScope)
	} else {
		projections, projectionsScope = b.buildProjectionList(sel.Exprs, groupingsScope)
	}

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

		outScope = groupingsScope
	} else {
		// No aggregates, so current output scope is the "from" scope.
		outScope = fromScope
	}

	if stmt.OrderBy != nil {
		// OrderBy can reference columns from either the from/grouping clause
		// or the projections clause, so combine them in a single projection.
		var orderByScope *scope
		out, orderByScope = b.buildAppendingProject(out, outScope, projections, projectionsScope)

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
		outScope = projectionsScope
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
	if groupBy == nil {
		return
	}

	outScope = inScope.push()

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
	if len(inScope.groupby.groupingsScope.groupby.aggs) > 0 {
		// If aggregates will be projected, check against them instead.
		inScope = inScope.groupby.groupingsScope
	}

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
	// We only have to handle "*" and "<name>.*" in the switch below. Other names
	// will be handled by scope.resolve().
	//
	// NB: The case statements are sorted lexicographically.
	switch t := projection.(type) {
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
