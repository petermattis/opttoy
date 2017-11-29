package v3

import (
	"fmt"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
					col.name = string(source.As.Cols[i])
				}
				col.table = tableName(source.As.Alias)
			}
		}
		return result

	case *tree.ParenTableExpr:
		return buildTable(source.Expr, scope)

	case *tree.JoinTableExpr:
		left := buildTable(source.Left, scope)
		right := buildTable(source.Right, scope.push(left.props))
		result := newJoinExpr(joinOp(source.Join), left, right)
		result.props = &relationalProps{}

		switch cond := source.Cond.(type) {
		case *tree.OnJoinCond:
			buildOnJoin(result, cond.Expr, scope)

		case tree.NaturalJoinCond:
			buildNaturalJoin(result)

		case *tree.UsingJoinCond:
			buildUsingJoin(result, cond.Cols)

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

	state := scope.state
	base, ok := state.tables[tab.name]
	if !ok {
		base = state.nextVar
		state.tables[tab.name] = base
		state.nextVar += bitmapIndex(len(tab.columns))
	}

	for i, col := range tab.columns {
		index := base + bitmapIndex(i)
		props.columns = append(props.columns, columnProps{
			index: index,
			name:  col.name,
			table: tab.name,
		})
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
	result.addFilter(buildScalar(on, scope.push(result.props)))
}

func buildNaturalJoin(e *expr) {
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
	buildUsingJoin(e, names)
}

func buildUsingJoin(e *expr, names tree.NameList) {
	inputs := e.inputs()
	left := inputs[0].props
	right := inputs[1].props
	e.props.columns = make([]columnProps, 0, len(left.columns)+len(right.columns))

	joined := make(map[string]*columnProps, len(names))
	for _, name := range names {
		name := string(name)
		// For every adjacent pair of tables, add an equality predicate.
		leftCol := left.findColumn(name)
		if leftCol == nil {
			fatalf("unable to resolve name %s", name)
		}
		rightCol := right.findColumn(name)
		if rightCol == nil {
			fatalf("unable to resolve name %s", name)
		}
		e.addFilter(newBinaryExpr(eqOp, leftCol.newVariableExpr(""), rightCol.newVariableExpr("")))
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

func buildScalar(pexpr tree.Expr, scope *scope) *expr {
	var result *expr
	switch t := pexpr.(type) {
	case *tree.ParenExpr:
		return buildScalar(t.Expr, scope)

	case *tree.AndExpr:
		result = newBinaryExpr(andOp, buildScalar(t.Left, scope), buildScalar(t.Right, scope))
	case *tree.OrExpr:
		result = newBinaryExpr(orOp, buildScalar(t.Left, scope), buildScalar(t.Right, scope))
	case *tree.NotExpr:
		result = newUnaryExpr(notOp, buildScalar(t.Expr, scope))

	case *tree.BinaryExpr:
		result = newBinaryExpr(binaryOpMap[t.Operator],
			buildScalar(t.Left, scope), buildScalar(t.Right, scope))
	case *tree.ComparisonExpr:
		result = newBinaryExpr(comparisonOpMap[t.Operator],
			buildScalar(t.Left, scope), buildScalar(t.Right, scope))
	case *tree.UnaryExpr:
		result = newUnaryExpr(unaryOpMap[t.Operator], buildScalar(t.Expr, scope))

	case *tree.ColumnItem:
		tblName := tableName(t.TableName.Table())
		colName := string(t.ColumnName)

		for s := scope; s != nil; s = s.parent {
			for _, col := range s.props.columns {
				if col.hasColumn(tblName, colName) {
					if tblName == "" && col.table != "" {
						t.TableName.TableName = tree.Name(col.table)
						t.TableName.DBNameOriginallyOmitted = true
					}
					return col.newVariableExpr("")
				}
			}
		}
		fatalf("unknown column %s", t)

	case tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return buildScalar(vn, scope)

	case *tree.NumVal:
		result = newConstExpr(t)

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(tree.SearchPath{})
		if err != nil {
			fatalf("%v", err)
		}
		children := make([]*expr, 0, len(t.Exprs))
		for _, pexpr := range t.Exprs {
			var e *expr
			if _, ok := pexpr.(tree.UnqualifiedStar); ok {
				e = newConstExpr(tree.NewDInt(1))
			} else {
				e = buildScalar(pexpr, scope)
			}
			children = append(children, e)
		}
		result = newFunctionExpr(def, children)

	case *tree.ExistsExpr:
		result = newUnaryExpr(existsOp, buildScalar(t.Subquery, scope))

	case *tree.Subquery:
		return build(t.Select, scope)

	default:
		// NB: we can't type assert on tree.dNull because the type is not
		// exported.
		if pexpr == tree.DNull {
			result = newConstExpr(pexpr)
		} else {
			unimplemented("%T", pexpr)
		}
	}
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

	// TODO(peter): case *tree.ValuesClause:

	default:
		unimplemented("%T", stmt.Select)
	}

	result = buildOrderBy(result, stmt.OrderBy, scope)
	// TODO(peter): stmt.Limit
	return result
}

func buildFrom(from *tree.From, where *tree.Where, scope *scope) (*expr, *scope) {
	if from == nil {
		return nil, scope
	}

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
		buildUsingJoin(result, nil)
		result.initProps()
		scope = scope.push(result.props)
	}

	if where != nil {
		input := result
		result = newSelectExpr(input)
		result.props = &relationalProps{
			columns: make([]columnProps, len(input.props.columns)),
		}
		copy(result.props.columns, input.props.columns)
		result.addFilter(buildScalar(where.Expr, scope))
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
		exprs = append(exprs, buildScalar(expr, scope))
	}
	result.addGroupings(exprs)
	result.initProps()

	if having != nil {
		f := buildScalar(having.Expr, scope)
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

		index := scope.state.nextVar
		scope.state.nextVar++
		name := fmt.Sprintf("column%d", len(g.props.columns)+1)
		g.props.columns = append(g.props.columns, columnProps{
			index: index,
			name:  name,
		})
		*e = *g.props.columns[len(g.props.columns)-1].newVariableExpr("")
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
		return []*expr{buildScalar(pexpr, scope)}
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

			name := string(sexpr.As)
			if p.op != variableOp {
				passthru = false
				index := scope.state.nextVar
				scope.state.nextVar++
				if name == "" {
					name = fmt.Sprintf("column%d", len(result.props.columns)+1)
				}
				p.scalarProps.definedCols.Add(index)
				result.props.columns = append(result.props.columns, columnProps{
					index: index,
					name:  name,
				})
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
		e := buildScalar(o.Expr, scope)
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
