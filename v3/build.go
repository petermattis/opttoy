package v3

import (
	"fmt"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

var comparisonOpMap = [...]operator{
	parser.EQ:                eqOp,
	parser.LT:                ltOp,
	parser.GT:                gtOp,
	parser.LE:                leOp,
	parser.GE:                geOp,
	parser.NE:                neOp,
	parser.In:                inOp,
	parser.NotIn:             notInOp,
	parser.Like:              likeOp,
	parser.NotLike:           notLikeOp,
	parser.ILike:             iLikeOp,
	parser.NotILike:          notILikeOp,
	parser.SimilarTo:         similarToOp,
	parser.NotSimilarTo:      notSimilarToOp,
	parser.RegMatch:          regMatchOp,
	parser.NotRegMatch:       notRegMatchOp,
	parser.RegIMatch:         regIMatchOp,
	parser.NotRegIMatch:      notRegIMatchOp,
	parser.IsDistinctFrom:    isDistinctFromOp,
	parser.IsNotDistinctFrom: isNotDistinctFromOp,
	parser.Is:                isOp,
	parser.IsNot:             isNotOp,
	parser.Any:               anyOp,
	parser.Some:              someOp,
	parser.All:               allOp,
}

var binaryOpMap = [...]operator{
	parser.Bitand:   bitandOp,
	parser.Bitor:    bitorOp,
	parser.Bitxor:   bitxorOp,
	parser.Plus:     plusOp,
	parser.Minus:    minusOp,
	parser.Mult:     multOp,
	parser.Div:      divOp,
	parser.FloorDiv: floorDivOp,
	parser.Mod:      modOp,
	parser.Pow:      powOp,
	parser.Concat:   concatOp,
	parser.LShift:   lShiftOp,
	parser.RShift:   rShiftOp,
}

var unaryOpMap = [...]operator{
	parser.UnaryPlus:       unaryPlusOp,
	parser.UnaryMinus:      unaryMinusOp,
	parser.UnaryComplement: unaryComplementOp,
}

type scope struct {
	parent *scope
	props  *logicalProps
	state  *queryState
}

func (s *scope) push(props *logicalProps) *scope {
	return &scope{
		parent: s,
		props:  props,
		state:  s.state,
	}
}

func build(stmt parser.Statement, scope *scope) *expr {
	switch stmt := stmt.(type) {
	case *parser.Select:
		return buildSelect(stmt, scope)
	case *parser.ParenSelect:
		return buildSelect(stmt.Select, scope)
	default:
		unimplemented("%T", stmt)
		return nil
	}
}

func buildTable(texpr parser.TableExpr, scope *scope) *expr {
	switch source := texpr.(type) {
	case *parser.NormalizableTableName:
		tableName, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}
		name := tableName.Table()
		tab, ok := scope.state.catalog[name]
		if !ok {
			fatalf("unknown table %s", name)
		}

		return buildScan(tab, scope)

	case *parser.AliasedTableExpr:
		result := buildTable(source.Expr, scope)
		if source.As.Alias != "" {
			if n := len(source.As.Cols); n > 0 && n != len(result.props.columns) {
				fatalf("rename specified %d columns, but table contains %d",
					n, len(result.props.columns))
			}

			tab := result.props
			result = &expr{
				op:       renameOp,
				children: []*expr{result},
				props: &logicalProps{
					columns: make([]columnProps, 0, len(tab.columns)),
				},
			}

			tables := []string{string(source.As.Alias)}
			for i, col := range tab.columns {
				name := col.name
				if i < len(source.As.Cols) {
					name = string(source.As.Cols[i])
				}

				result.props.columns = append(result.props.columns, columnProps{
					index:  col.index,
					name:   name,
					tables: tables,
				})
			}

			result.updateProps()
			return result
		}
		return result

	case *parser.ParenTableExpr:
		return buildTable(source.Expr, scope)

	case *parser.JoinTableExpr:
		left := buildTable(source.Left, scope)
		right := buildTable(source.Right, scope.push(left.props))
		result := &expr{
			op: joinOp(source.Join),
			children: []*expr{
				left,
				right,
			},
		}

		switch cond := source.Cond.(type) {
		case *parser.OnJoinCond:
			buildOnJoin(result, cond.Expr, scope)

		case parser.NaturalJoinCond:
			buildNaturalJoin(result)

		case *parser.UsingJoinCond:
			buildUsingJoin(result, cond.Cols)

		default:
			unimplemented("%T", source.Cond)
		}

		result.updateProps()
		return result

	case *parser.Subquery:
		return build(source.Select, scope)

	default:
		unimplemented("%T", texpr)
		return nil
	}
}

func buildScan(tab *table, scope *scope) *expr {
	result := &expr{
		op:      scanOp,
		props:   &logicalProps{},
		private: tab,
	}

	props := result.props
	props.columns = make([]columnProps, 0, len(tab.columns))

	state := scope.state
	base, ok := state.tables[tab.name]
	if !ok {
		base = state.nextVar
		state.tables[tab.name] = base
		state.nextVar += bitmapIndex(len(tab.columns))
	}

	tables := []string{tab.name}
	for i, col := range tab.columns {
		index := base + bitmapIndex(i)
		props.columns = append(props.columns, columnProps{
			index:  index,
			name:   col.name,
			tables: tables,
		})
	}

	// Initialize keys from the table schema.
	for _, k := range tab.keys {
		if k.fkey == nil && (k.primary || k.unique) {
			var key bitmap
			for _, i := range k.columns {
				key.set(props.columns[i].index)
			}
			props.weakKeys = append(props.weakKeys, key)
		}
	}

	// Initialize not-NULL columns from the table schema.
	for i, col := range tab.columns {
		if col.notNull {
			props.notNullCols.set(props.columns[i].index)
		}
	}

	result.updateProps()
	return result
}

func buildOnJoin(result *expr, on parser.Expr, scope *scope) {
	left := result.inputs()[0].props
	right := result.inputs()[1].props
	result.props = &logicalProps{
		columns: make([]columnProps, len(left.columns)+len(right.columns)),
	}
	copy(result.props.columns[:], left.columns)
	copy(result.props.columns[len(left.columns):], right.columns)
	result.addFilter(buildScalar(on, scope.push(result.props)))
}

func buildNaturalJoin(e *expr) {
	inputs := e.inputs()
	names := make(parser.NameList, 0, len(inputs[0].props.columns))
	for _, col := range inputs[0].props.columns {
		names = append(names, parser.Name(col.name))
	}
	for _, input := range inputs[1:] {
		var common parser.NameList
		for _, colName := range names {
			for _, col := range input.props.columns {
				if colName == parser.Name(col.name) {
					common = append(common, colName)
				}
			}
		}
		names = common
	}
	buildUsingJoin(e, names)
}

func buildUsingJoin(e *expr, names parser.NameList) {
	joined := make(map[string]int, len(names))
	inputs := e.inputs()
	for _, name := range names {
		joined[string(name)] = -1
		// For every adjacent pair of tables, add an equality predicate.
		for i := 1; i < len(inputs); i++ {
			left := inputs[i-1].props.newColumnExpr(string(name))
			if left == nil {
				fatalf("unable to resolve name %s", name)
			}
			right := inputs[i].props.newColumnExpr(string(name))
			if right == nil {
				fatalf("unable to resolve name %s", name)
			}
			f := &expr{
				op: eqOp,
				children: []*expr{
					left,
					right,
				},
			}
			f.updateProps()
			e.addFilter(f)
		}
	}

	e.props = &logicalProps{}
	for _, input := range inputs {
		for _, col := range input.props.columns {
			if idx, ok := joined[col.name]; ok {
				if idx != -1 {
					oldCol := e.props.columns[idx]
					e.props.columns[idx] = columnProps{
						index:  oldCol.index,
						name:   oldCol.name,
						tables: append(oldCol.tables, col.tables[0]),
					}
					continue
				}
				joined[col.name] = len(e.props.columns)
			}

			e.props.columns = append(e.props.columns, columnProps{
				index:  col.index,
				name:   col.name,
				tables: []string{col.tables[0]},
			})
		}
	}
}

func buildLeftOuterJoin(e *expr) {
	left := e.inputs()[0].props
	right := e.inputs()[1].props
	e.props = &logicalProps{
		columns: make([]columnProps, len(left.columns)+len(right.columns)),
	}
	copy(e.props.columns[:], left.columns)
	copy(e.props.columns[len(left.columns):], right.columns)
}

func buildScalar(pexpr parser.Expr, scope *scope) *expr {
	var result *expr
	switch t := pexpr.(type) {
	case *parser.ParenExpr:
		return buildScalar(t.Expr, scope)

	case *parser.AndExpr:
		result = &expr{
			op: andOp,
			children: []*expr{
				buildScalar(t.Left, scope),
				buildScalar(t.Right, scope),
			},
		}
	case *parser.OrExpr:
		result = &expr{
			op: orOp,
			children: []*expr{
				buildScalar(t.Left, scope),
				buildScalar(t.Right, scope),
			},
		}
	case *parser.NotExpr:
		result = &expr{
			op: notOp,
			children: []*expr{
				buildScalar(t.Expr, scope),
			},
		}

	case *parser.BinaryExpr:
		result = &expr{
			op: binaryOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Left, scope),
				buildScalar(t.Right, scope),
			},
		}
	case *parser.ComparisonExpr:
		result = &expr{
			op: comparisonOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Left, scope),
				buildScalar(t.Right, scope),
			},
		}
	case *parser.UnaryExpr:
		result = &expr{
			op: unaryOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Expr, scope),
			},
		}

	case *parser.ColumnItem:
		tableName := t.TableName.Table()
		colName := string(t.ColumnName)

		for s := scope; s != nil; s = s.parent {
			for _, col := range s.props.columns {
				if col.hasColumn(tableName, colName) {
					if tableName == "" && len(col.tables) > 0 {
						t.TableName.TableName = parser.Name(col.tables[0])
						t.TableName.DBNameOriginallyOmitted = true
					}
					result = &expr{
						op:      variableOp,
						props:   s.props,
						private: t,
					}
					result.inputVars.set(col.index)
					result.updateProps()
					return result
				}
			}
		}
		fatalf("unknown column %s", t)

	case parser.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return buildScalar(vn, scope)

	case *parser.NumVal:
		result = &expr{
			op:      constOp,
			props:   scope.props,
			private: t,
		}

	case *parser.FuncExpr:
		def, err := t.Func.Resolve(parser.SearchPath{})
		if err != nil {
			fatalf("%v", err)
		}
		result = &expr{
			op:      functionOp,
			props:   scope.props,
			private: def,
		}
		result.children = make([]*expr, 0, len(t.Exprs))
		for _, pexpr := range t.Exprs {
			var e *expr
			if _, ok := pexpr.(parser.UnqualifiedStar); ok {
				e = &expr{
					op:      constOp,
					props:   scope.props,
					private: parser.NewDInt(1),
				}
			} else {
				e = buildScalar(pexpr, scope)
			}
			result.children = append(result.children, e)
		}

	case *parser.ExistsExpr:
		result = &expr{
			op: existsOp,
			children: []*expr{
				buildScalar(t.Subquery, scope),
			},
		}

	case *parser.Subquery:
		return build(t.Select, scope)

	default:
		// NB: we can't type assert on parser.dNull because the type is not
		// exported.
		if pexpr == parser.DNull {
			result = &expr{
				op:      constOp,
				props:   scope.props,
				private: pexpr,
			}
		} else {
			unimplemented("%T", pexpr)
		}
	}
	result.updateProps()
	return result
}

func buildSelect(stmt *parser.Select, scope *scope) *expr {
	var result *expr

	switch t := stmt.Select.(type) {
	case *parser.SelectClause:
		result, scope = buildFrom(t.From, t.Where, scope)
		result, scope = buildGroupBy(result, t.GroupBy, t.Having, scope)
		result, scope = buildProjections(result, t.Exprs, scope)
		result, scope = buildDistinct(result, t.Distinct, scope)

	case *parser.UnionClause:
		result = buildUnion(t, scope)

	case *parser.ParenSelect:
		result = buildSelect(t.Select, scope)

	// TODO(peter): case *parser.ValuesClause:

	default:
		unimplemented("%T", stmt.Select)
	}

	result = buildOrderBy(result, stmt.OrderBy, scope)
	// TODO(peter): stmt.Limit
	return result
}

func buildFrom(from *parser.From, where *parser.Where, scope *scope) (*expr, *scope) {
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
		result = &expr{
			op: innerJoinOp,
			children: []*expr{
				result,
				t,
			},
		}
		buildUsingJoin(result, nil)
		result.updateProps()
		scope = scope.push(result.props)
	}

	if where != nil {
		result.addFilter(buildScalar(where.Expr, scope))
		result.updateProps()
	}

	return result, scope
}

func buildGroupBy(
	input *expr,
	groupBy parser.GroupBy,
	having *parser.Where,
	scope *scope,
) (*expr, *scope) {
	if groupBy == nil {
		return input, scope
	}

	result := &expr{
		op:       groupByOp,
		children: []*expr{input},
		props: &logicalProps{
			columns: make([]columnProps, len(scope.props.columns)),
		},
	}
	copy(result.props.columns, scope.props.columns)

	exprs := make([]*expr, 0, len(groupBy))
	for _, expr := range groupBy {
		exprs = append(exprs, buildScalar(expr, scope))
	}
	result.addGroupings(exprs)

	if having != nil {
		f := buildScalar(having.Expr, scope)
		buildGroupByExtractAggregates(result, f, scope)
		result.addFilter(f)
	}

	result.updateProps()
	return result, scope
}

func buildGroupByExtractAggregates(g *expr, e *expr, scope *scope) bool {
	if isAggregate(e) {
		// Check to see if the aggregation already exists.
		for i, a := range g.aggregations() {
			if equivalent(a, e) {
				col := g.props.columns[i+len(g.inputs()[0].props.columns)]
				*e = *col.newVariableExpr("", g.props)
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
		*e = *g.props.columns[len(g.props.columns)-1].newVariableExpr("", g.props)
		return true
	}

	var res bool
	for _, input := range e.inputs() {
		res = buildGroupByExtractAggregates(g, input, scope) || res
	}
	if res {
		e.updateProps()
	}
	return res
}

func buildProjection(pexpr parser.Expr, scope *scope) []*expr {
	switch t := pexpr.(type) {
	case parser.UnqualifiedStar:
		var projections []*expr
		for _, col := range scope.props.columns {
			projections = append(projections, col.newVariableExpr("", scope.props))
		}
		if len(projections) == 0 {
			fatalf("failed to expand *")
		}
		return projections

	case *parser.AllColumnsSelector:
		tableName := t.TableName.Table()
		var projections []*expr
		for _, col := range scope.props.columns {
			if col.hasTable(tableName) {
				projections = append(projections, col.newVariableExpr(tableName, scope.props))
			}
		}
		if len(projections) == 0 {
			fatalf("unknown table %s", t)
		}
		return projections

	case parser.UnresolvedName:
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
	sexprs parser.SelectExprs,
	scope *scope,
) (*expr, *scope) {
	if len(sexprs) == 0 {
		return input, scope
	}

	result := &expr{
		op:       projectOp,
		children: []*expr{input},
		props:    &logicalProps{},
	}

	var projections []*expr
	passthru := true
	for _, sexpr := range sexprs {
		exprs := buildProjection(sexpr.Expr, scope)
		projections = append(projections, exprs...)

		for _, p := range exprs {
			if containsAggregate(p) {
				if input.op != groupByOp {
					input = &expr{
						op:       groupByOp,
						children: []*expr{input},
						props:    &logicalProps{},
					}
					result.inputs()[0] = input
				}
				buildGroupByExtractAggregates(input, p, scope)
				input.updateProps()
			}

			name := string(sexpr.As)
			var tables []string

			var index bitmapIndex
			if p.op != variableOp {
				index = scope.state.nextVar
				scope.state.nextVar++
				if name == "" {
					name = fmt.Sprintf("column%d", len(result.props.columns)+1)
				}
			} else {
				index = bitmapIndex(bits.TrailingZeros64(uint64(p.inputVars)))
				for j, col := range input.props.columns {
					if index == col.index {
						if name == "" {
							name = col.name
							passthru = passthru && j == len(result.props.columns)
						} else {
							passthru = false
						}
						tables = col.tables
						break
					}
				}
			}

			result.props.columns = append(result.props.columns, columnProps{
				index:  index,
				name:   name,
				tables: tables,
			})
		}
	}

	// Don't add an unnecessary "pass through" project expression.
	if len(result.props.columns) == len(input.props.columns) && passthru {
		return input, scope
	}

	result.addProjections(projections)
	result.updateProps()
	return result, scope.push(result.props)
}

func buildDistinct(input *expr, distinct bool, scope *scope) (*expr, *scope) {
	if !distinct {
		return input, scope
	}

	// Distint is equivalent to group by without any aggregations.
	result := &expr{
		op:       groupByOp,
		children: []*expr{input},
		props: &logicalProps{
			columns: make([]columnProps, len(scope.props.columns)),
		},
	}
	copy(result.props.columns, scope.props.columns)

	exprs := make([]*expr, 0, len(input.props.columns))
	for _, col := range input.props.columns {
		exprs = append(exprs, col.newVariableExpr("", input.props))
	}
	result.addGroupings(exprs)

	result.updateProps()
	return result, scope
}

func buildOrderBy(input *expr, orderBy parser.OrderBy, scope *scope) *expr {
	if orderBy == nil {
		return input
	}

	// TODO(peter): order by is not a relational expression, but instead a
	// required property on the output.
	result := &expr{
		op:       orderByOp,
		children: []*expr{input},
		props: &logicalProps{
			columns: make([]columnProps, len(input.props.columns)),
		},
		private: orderBy,
	}
	copy(result.props.columns, input.props.columns)
	result.updateProps()
	return result
}

func buildUnion(clause *parser.UnionClause, scope *scope) *expr {
	op := unionOp
	switch clause.Type {
	case parser.UnionOp:
	case parser.IntersectOp:
		op = intersectOp
	case parser.ExceptOp:
		op = exceptOp
	}
	left := buildSelect(clause.Left, scope)
	right := buildSelect(clause.Right, scope)
	result := &expr{
		op: op,
		children: []*expr{
			left,
			right,
		},
		props: &logicalProps{
			columns: make([]columnProps, len(left.props.columns)),
		},
	}
	copy(result.props.columns, left.props.columns)
	result.updateProps()
	return result
}
