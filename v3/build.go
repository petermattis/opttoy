package v3

import (
	"fmt"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var comparisonOpMap = [...]operator{
	tree.EQ:                eqOp,
	tree.LT:                ltOp,
	tree.GT:                gtOp,
	tree.LE:                leOp,
	tree.GE:                geOp,
	tree.NE:                neOp,
	tree.In:                inOp,
	tree.NotIn:             notInOp,
	tree.Like:              likeOp,
	tree.NotLike:           notLikeOp,
	tree.ILike:             iLikeOp,
	tree.NotILike:          notILikeOp,
	tree.SimilarTo:         similarToOp,
	tree.NotSimilarTo:      notSimilarToOp,
	tree.RegMatch:          regMatchOp,
	tree.NotRegMatch:       notRegMatchOp,
	tree.RegIMatch:         regIMatchOp,
	tree.NotRegIMatch:      notRegIMatchOp,
	tree.IsDistinctFrom:    isDistinctFromOp,
	tree.IsNotDistinctFrom: isNotDistinctFromOp,
	tree.Is:                isOp,
	tree.IsNot:             isNotOp,
	tree.Any:               anyOp,
	tree.Some:              someOp,
	tree.All:               allOp,
}

var binaryOpMap = [...]operator{
	tree.Bitand:   bitandOp,
	tree.Bitor:    bitorOp,
	tree.Bitxor:   bitxorOp,
	tree.Plus:     plusOp,
	tree.Minus:    minusOp,
	tree.Mult:     multOp,
	tree.Div:      divOp,
	tree.FloorDiv: floorDivOp,
	tree.Mod:      modOp,
	tree.Pow:      powOp,
	tree.Concat:   concatOp,
	tree.LShift:   lShiftOp,
	tree.RShift:   rShiftOp,
}

var unaryOpMap = [...]operator{
	tree.UnaryPlus:       unaryPlusOp,
	tree.UnaryMinus:      unaryMinusOp,
	tree.UnaryComplement: unaryComplementOp,
}

func build(stmt tree.Statement, scope *scope) *expr {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return buildSelect(stmt, scope)
	case *tree.ParenSelect:
		return buildSelect(stmt.Select, scope)
	default:
		unimplemented("%T", stmt)
		return nil
	}
}

func buildTable(texpr tree.TableExpr, scope *scope) *expr {
	switch source := texpr.(type) {
	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}
		name := tableName(tn.Table())
		tab, ok := scope.state.catalog[name]
		if !ok {
			fatalf("unknown table %s", name)
		}

		return buildScan(tab, scope)

	case *tree.AliasedTableExpr:
		result := buildTable(source.Expr, scope)
		if source.As.Alias != "" {
			if n := len(source.As.Cols); n > 0 && n != len(result.props.columns) {
				fatalf("rename specified %d columns, but table contains %d",
					n, len(result.props.columns))
			}

			for i := range result.props.columns {
				col := &result.props.columns[i]
				if i < len(source.As.Cols) {
					col.name = columnName(source.As.Cols[i])
				}
				col.table = tableName(source.As.Alias)
			}
		}
		return result

	case *tree.ParenTableExpr:
		return buildTable(source.Expr, scope)

	case *tree.JoinTableExpr:
		left := buildTable(source.Left, scope)
		scope = scope.push(left.props)
		right := buildTable(source.Right, scope)
		scope = scope.push(right.props)
		result := newJoinExpr(joinOp(source.Join), left, right)
		result.props = &relationalProps{}

		switch cond := source.Cond.(type) {
		case *tree.OnJoinCond:
			buildOnJoin(result, cond.Expr, scope)

		case tree.NaturalJoinCond:
			buildNaturalJoin(result, scope)

		case *tree.UsingJoinCond:
			buildUsingJoin(result, cond.Cols, scope)

		default:
			unimplemented("%T", source.Cond)
		}

		result.initProps()
		return result

	case *tree.Subquery:
		return build(source.Select, scope)

	default:
		unimplemented("%T", texpr)
		return nil
	}
}

func buildScan(tab *table, scope *scope) *expr {
	result := newScanExpr(tab)
	result.props = &relationalProps{
		columns: make([]columnProps, 0, len(tab.columns)),
	}
	props := result.props

	// Every reference to a table in the query gets a new set of output column
	// indexes. Consider the query:
	//
	//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
	//
	// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not
	// equivalent to `r.y`. In order to achieve this, we need to give these
	// columns different indexes.
	state := scope.state
	base := bitmapIndex(len(state.columns))

	// TODO(peter): queryState.tables is used for looking up foreign key
	// references. Currently, this lookup is global, but it likely needs to be
	// scoped.
	if _, ok := state.tables[tab.name]; !ok {
		state.tables[tab.name] = base
	}

	for i, col := range tab.columns {
		index := base + bitmapIndex(i)
		col := columnProps{
			index: index,
			name:  col.name,
			table: tab.name,
			typ:   col.typ,
		}
		state.columns = append(state.columns, col)
		props.columns = append(props.columns, col)
	}

	// Initialize keys from the table schema.
	for _, k := range tab.keys {
		if k.fkey == nil && (k.primary || k.unique) {
			var key bitmap
			for _, i := range k.columns {
				key.Add(props.columns[i].index)
			}
			props.weakKeys = append(props.weakKeys, key)
		}
	}

	// Initialize not-NULL columns from the table schema.
	for i, col := range tab.columns {
		if col.notNull {
			props.notNullCols.Add(props.columns[i].index)
		}
	}

	result.initProps()
	return result
}

func buildOnJoin(result *expr, on tree.Expr, scope *scope) {
	left := result.inputs()[0].props
	right := result.inputs()[1].props
	result.props.columns = make([]columnProps, len(left.columns)+len(right.columns))
	copy(result.props.columns[:], left.columns)
	copy(result.props.columns[len(left.columns):], right.columns)
	scope = scope.push(result.props)
	result.addFilter(buildScalar(scope.resolve(on, types.Bool), scope))
}

func buildNaturalJoin(e *expr, scope *scope) {
	inputs := e.inputs()
	names := make(tree.NameList, 0, len(inputs[0].props.columns))
	for _, col := range inputs[0].props.columns {
		if !col.hidden {
			names = append(names, tree.Name(col.name))
		}
	}
	for _, input := range inputs[1:] {
		var common tree.NameList
		for _, colName := range names {
			for _, col := range input.props.columns {
				if !col.hidden && colName == tree.Name(col.name) {
					common = append(common, colName)
					break
				}
			}
		}
		names = common
	}
	buildUsingJoin(e, names, scope)
}

func buildUsingJoin(e *expr, names tree.NameList, scope *scope) {
	inputs := e.inputs()
	left := inputs[0].props
	right := inputs[1].props
	e.props.columns = make([]columnProps, 0, len(left.columns)+len(right.columns))

	joined := make(map[columnName]*columnProps, len(names))
	for _, name := range names {
		name := columnName(name)
		// For every adjacent pair of tables, add an equality predicate.
		leftCol := left.findColumn(name)
		if leftCol == nil {
			fatalf("unable to resolve name %s", name)
		}
		rightCol := right.findColumn(name)
		if rightCol == nil {
			fatalf("unable to resolve name %s", name)
		}
		// Build a tree.ComparisonExpr in order to use the normal type checking
		// machinery.
		cmp := &tree.ComparisonExpr{
			Operator: tree.EQ,
			Left:     tree.NewIndexedVar(leftCol.index),
			Right:    tree.NewIndexedVar(rightCol.index),
		}
		e.addFilter(buildScalar(scope.resolve(cmp, types.Bool), scope))
		e.props.columns = append(e.props.columns, *leftCol)
		joined[name] = leftCol
	}

	for _, col := range left.columns {
		jcol, ok := joined[col.name]
		if ok {
			if col == *jcol {
				continue
			}
			col.hidden = true
		}
		e.props.columns = append(e.props.columns, col)
	}
	for _, col := range right.columns {
		_, col.hidden = joined[col.name]
		e.props.columns = append(e.props.columns, col)
	}
}

func buildLeftOuterJoin(e *expr) {
	left := e.inputs()[0].props
	right := e.inputs()[1].props
	e.props.columns = make([]columnProps, len(left.columns)+len(right.columns))
	copy(e.props.columns[:], left.columns)
	copy(e.props.columns[len(left.columns):], right.columns)
}

func buildScalar(pexpr tree.TypedExpr, scope *scope) *expr {
	var result *expr
	switch t := pexpr.(type) {
	case *tree.Tuple:
		result = &expr{
			op:          tupleOp,
			children:    make([]*expr, len(t.Exprs)),
			scalarProps: &scalarProps{},
		}
		for i := range t.Exprs {
			result.children[i] = buildScalar(t.Exprs[i].(tree.TypedExpr), scope)
		}

	case *tree.ParenExpr:
		return buildScalar(t.TypedInnerExpr(), scope)

	case *tree.AndExpr:
		result = newBinaryExpr(andOp,
			buildScalar(t.TypedLeft(), scope),
			buildScalar(t.TypedRight(), scope))
	case *tree.OrExpr:
		result = newBinaryExpr(orOp,
			buildScalar(t.TypedLeft(), scope),
			buildScalar(t.TypedRight(), scope))
	case *tree.NotExpr:
		result = newUnaryExpr(notOp,
			buildScalar(t.TypedInnerExpr(), scope))

	case *tree.BinaryExpr:
		result = newBinaryExpr(binaryOpMap[t.Operator],
			buildScalar(t.TypedLeft(), scope),
			buildScalar(t.TypedRight(), scope))
	case *tree.ComparisonExpr:
		result = newBinaryExpr(comparisonOpMap[t.Operator],
			buildScalar(t.TypedLeft(), scope),
			buildScalar(t.TypedRight(), scope))
	case *tree.UnaryExpr:
		result = newUnaryExpr(unaryOpMap[t.Operator],
			buildScalar(t.TypedInnerExpr(), scope))

	case *tree.IndexedVar:
		result = scope.newVariableExpr(t.Idx)
		if result == nil {
			panic(fmt.Errorf("unable to find indexed var @%d", t.Idx))
		}
		return result

	case *tree.Placeholder:
		result = &expr{
			op:          placeholderOp,
			scalarProps: &scalarProps{},
			private:     t,
		}

	case tree.Datum:
		result = newConstExpr(t)

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(scope.state.semaCtx.SearchPath)
		if err != nil {
			fatalf("%v", err)
		}
		children := make([]*expr, 0, len(t.Exprs))
		for _, pexpr := range t.Exprs {
			var e *expr
			if _, ok := pexpr.(tree.UnqualifiedStar); ok {
				e = newConstExpr(tree.NewDInt(1))
			} else {
				e = buildScalar(pexpr.(tree.TypedExpr), scope)
			}
			children = append(children, e)
		}
		result = newFunctionExpr(def, children)

	case *tree.ExistsExpr:
		// TODO(peter): the decorrelation code currently expects the subquery to be
		// unwrapped for EXISTS expressions.
		subquery := t.Subquery.(*subquery)
		result = newUnaryExpr(existsOp, subquery.expr)

	case *subquery:
		result = newUnaryExpr(subqueryOp, t.expr)

	default:
		unimplemented("%T", pexpr)
	}

	result.scalarProps.typ = pexpr.ResolvedType()
	return result
}

func buildSelect(stmt *tree.Select, scope *scope) *expr {
	var result *expr

	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		result, scope = buildFrom(t.From, t.Where, scope)
		result, scope = buildGroupBy(result, t.GroupBy, t.Having, scope)
		result, scope = buildProjections(result, t.Exprs, scope)
		result, scope = buildDistinct(result, t.Distinct, scope)

	case *tree.UnionClause:
		result = buildUnion(t, scope)

	case *tree.ParenSelect:
		result = buildSelect(t.Select, scope)

	case *tree.ValuesClause:
		var numCols int
		if len(t.Tuples) > 0 {
			numCols = len(t.Tuples[0].Exprs)
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
		}

	default:
		unimplemented("%T", stmt.Select)
	}

	result = buildOrderBy(result, stmt.OrderBy, scope)
	// TODO(peter): stmt.Limit
	return result
}

func buildFrom(from *tree.From, where *tree.Where, scope *scope) (*expr, *scope) {
	var result *expr
	for _, table := range from.Tables {
		t := buildTable(table, scope)
		if result == nil {
			result = t
			scope = scope.push(result.props)
			continue
		}
		result = newJoinExpr(innerJoinOp, result, t)
		result.props = &relationalProps{}
		buildUsingJoin(result, nil, scope)
		result.initProps()
		scope = scope.push(result.props)
	}

	if result == nil {
		// TODO(peter): This should be a table with 1 row and 0 columns to match
		// current cockroach behavior.
		result = &expr{
			op:    valuesOp,
			props: &relationalProps{},
		}
	}

	if where != nil {
		input := result
		result = newSelectExpr(input)
		result.props = &relationalProps{
			columns: make([]columnProps, len(input.props.columns)),
		}
		copy(result.props.columns, input.props.columns)
		texpr := scope.resolve(where.Expr, types.Bool)
		result.addFilter(buildScalar(texpr, scope))
		result.initProps()
		scope = scope.push(result.props)
	}

	return result, scope
}

func buildGroupBy(
	input *expr,
	groupBy tree.GroupBy,
	having *tree.Where,
	scope *scope,
) (*expr, *scope) {
	if groupBy == nil {
		return input, scope
	}

	result := newGroupByExpr(input)
	result.props = &relationalProps{
		columns: make([]columnProps, len(scope.props.columns)),
	}
	copy(result.props.columns, scope.props.columns)

	exprs := make([]*expr, 0, len(groupBy))
	for _, expr := range groupBy {
		texpr := scope.resolve(expr, types.Any)
		exprs = append(exprs, buildScalar(texpr, scope))
	}
	result.addGroupings(exprs)
	result.initProps()

	if having != nil {
		texpr := scope.resolve(having.Expr, types.Bool)
		f := buildScalar(texpr, scope)
		buildGroupByExtractAggregates(result, f, scope)
		result.initProps()

		input := result
		result = newSelectExpr(input)
		result.props = &relationalProps{
			columns: make([]columnProps, len(input.props.columns)),
		}
		copy(result.props.columns, input.props.columns)
		result.addFilter(f)
		result.initProps()
		scope = scope.push(result.props)
	}

	return result, scope
}

func buildGroupByExtractAggregates(g *expr, e *expr, scope *scope) bool {
	if isAggregate(e) {
		// Check to see if the aggregation already exists.
		for i, a := range g.aggregations() {
			if a.equal(e) {
				col := g.props.columns[i+len(g.inputs()[0].props.columns)]
				*e = *col.newVariableExpr("")
				return true
			}
		}

		t := *e
		g.addAggregation(&t)

		index := bitmapIndex(len(scope.state.columns))
		name := columnName(fmt.Sprintf("column%d", len(g.props.columns)+1))
		col := columnProps{
			index: index,
			name:  name,
			typ:   e.scalarProps.typ,
		}
		scope.state.columns = append(scope.state.columns, col)
		g.props.columns = append(g.props.columns, col)
		*e = *col.newVariableExpr("")
		return true
	}

	var res bool
	for _, input := range e.inputs() {
		res = buildGroupByExtractAggregates(g, input, scope) || res
	}
	if res {
		e.initProps()
	}
	return res
}

func buildProjection(pexpr tree.Expr, scope *scope) []*expr {
	switch t := pexpr.(type) {
	case tree.UnqualifiedStar:
		var projections []*expr
		for _, col := range scope.props.columns {
			if !col.hidden {
				projections = append(projections, col.newVariableExpr(""))
			}
		}
		if len(projections) == 0 {
			fatalf("failed to expand *")
		}
		return projections

	case *tree.AllColumnsSelector:
		tableName := tableName(t.TableName.Table())
		var projections []*expr
		for _, col := range scope.props.columns {
			if !col.hidden && col.table == tableName {
				projections = append(projections, col.newVariableExpr(tableName))
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
		return buildProjection(vn, scope)

	default:
		texpr := scope.resolve(pexpr, types.Any)
		return []*expr{buildScalar(texpr, scope)}
	}
}

func buildProjections(
	input *expr,
	sexprs tree.SelectExprs,
	scope *scope,
) (*expr, *scope) {
	if len(sexprs) == 0 {
		return input, scope
	}

	result := newProjectExpr(input)
	result.props = &relationalProps{}

	var projections []*expr
	passthru := true
	for _, sexpr := range sexprs {
		exprs := buildProjection(sexpr.Expr, scope)
		projections = append(projections, exprs...)

		for _, p := range exprs {
			if containsAggregate(p) {
				if input.op != groupByOp {
					input = newGroupByExpr(input)
					input.props = &relationalProps{}
					result.inputs()[0] = input
				}
				buildGroupByExtractAggregates(input, p, scope)
				input.initProps()
			}

			name := columnName(sexpr.As)
			if p.op != variableOp {
				passthru = false
				index := bitmapIndex(len(scope.state.columns))
				if name == "" {
					name = columnName(fmt.Sprintf("column%d", len(result.props.columns)+1))
				}
				col := columnProps{
					index: index,
					name:  name,
					typ:   p.scalarProps.typ,
				}
				scope.state.columns = append(scope.state.columns, col)
				result.props.columns = append(result.props.columns, col)
			} else {
				col := p.private.(columnProps)
				for j := range input.props.columns {
					if col == input.props.columns[j] {
						passthru = passthru && j == len(result.props.columns)
						break
					}
				}
				if name != "" {
					col.name = name
					passthru = false
				}
				result.props.columns = append(result.props.columns, col)
			}
		}
	}

	// Don't add an unnecessary "pass through" project expression.
	if len(result.props.columns) == len(input.props.columns) && passthru {
		return input, scope
	}

	result.addProjections(projections)
	result.initProps()
	return result, scope.push(result.props)
}

func buildDistinct(input *expr, distinct bool, scope *scope) (*expr, *scope) {
	if !distinct {
		return input, scope
	}

	// Distinct is equivalent to group by without any aggregations.
	result := newGroupByExpr(input)
	result.props = &relationalProps{
		columns: make([]columnProps, len(scope.props.columns)),
	}
	copy(result.props.columns, scope.props.columns)

	exprs := make([]*expr, 0, len(input.props.columns))
	for _, col := range input.props.columns {
		exprs = append(exprs, col.newVariableExpr(""))
	}
	result.addGroupings(exprs)

	result.initProps()
	return result, scope
}

func buildOrderBy(input *expr, orderBy tree.OrderBy, scope *scope) *expr {
	if orderBy == nil {
		return input
	}

	// Order-by is not a relational expression, but instead a required property
	// on the output. We add an orderByOp to the expression tree, and specify the
	// required ordering in the physical properties. Prep will later extract the
	// top-level ordering and pass that as a requirement to search.
	result := newOrderByExpr(input)
	result.props = &relationalProps{
		columns: make([]columnProps, len(input.props.columns)),
	}
	copy(result.props.columns, input.props.columns)
	result.initProps()

	ordering := make(ordering, 0, len(orderBy))
	for _, o := range orderBy {
		texpr := scope.resolve(o.Expr, types.Any)
		e := buildScalar(texpr, scope)
		switch e.op {
		case variableOp:
			index := e.private.(columnProps).index
			if o.Direction == tree.Descending {
				index = -(index + 1)
			}
			ordering = append(ordering, index)
		default:
			unimplemented("unsupported order-by: %s", o.Expr)
		}
	}
	result.physicalProps = &physicalProps{
		providedOrdering: ordering,
	}
	return result
}

func buildUnion(clause *tree.UnionClause, scope *scope) *expr {
	op := unionOp
	switch clause.Type {
	case tree.UnionOp:
	case tree.IntersectOp:
		op = intersectOp
	case tree.ExceptOp:
		op = exceptOp
	}
	left := buildSelect(clause.Left, scope)
	right := buildSelect(clause.Right, scope)
	result := newSetExpr(op, left, right)
	result.props = &relationalProps{
		columns: make([]columnProps, len(left.props.columns)),
	}
	copy(result.props.columns, left.props.columns)
	result.initProps()
	return result
}
