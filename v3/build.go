package v3

import (
	"fmt"

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

func build(
	stmt parser.Statement,
	tab *logicalProps) *expr {
	switch stmt := stmt.(type) {
	case *parser.Select:
		return buildSelect(stmt, tab)
	case *parser.ParenSelect:
		return buildSelect(stmt.Select, tab)
	default:
		unimplemented("%T", stmt)
		return nil
	}
}

func buildTable(texpr parser.TableExpr, props *logicalProps) *expr {
	switch source := texpr.(type) {
	case *parser.NormalizableTableName:
		tableName, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}
		name := tableName.Table()
		state := props.state
		tab, ok := state.catalog[name]
		if !ok {
			fatalf("unknown table %s", name)
		}

		result := &expr{
			op:        scanOp,
			dataIndex: state.addData(tab),
			props: &logicalProps{
				state: state,
			},
		}
		result.updateProps()
		return result

	case *parser.AliasedTableExpr:
		result := buildTable(source.Expr, props)
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
					state:   tab.state,
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
		return buildTable(source.Expr, props)

	case *parser.JoinTableExpr:
		result := &expr{
			op: innerJoinOp,
			children: []*expr{
				buildTable(source.Left, props),
				buildTable(source.Right, props),
			},
		}

		switch cond := source.Cond.(type) {
		case *parser.OnJoinCond:
			buildOnJoin(result, cond.Expr)

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
		return build(source.Select, props)

	default:
		unimplemented("%T", texpr)
		return nil
	}
}

func buildOnJoin(result *expr, on parser.Expr) {
	left := result.inputs()[0].props
	right := result.inputs()[1].props
	result.props = &logicalProps{
		columns: make([]columnProps, len(left.columns)+len(right.columns)),
		state:   left.state,
	}
	copy(result.props.columns[:], left.columns)
	copy(result.props.columns[len(left.columns):], right.columns)
	result.addFilter(buildScalar(on, result.props))
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

	e.props = &logicalProps{state: inputs[0].props.state}
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

func buildScalar(pexpr parser.Expr, props *logicalProps) *expr {
	var result *expr
	switch t := pexpr.(type) {
	case *parser.ParenExpr:
		return buildScalar(t.Expr, props)

	case *parser.AndExpr:
		result = &expr{
			op: andOp,
			children: []*expr{
				buildScalar(t.Left, props),
				buildScalar(t.Right, props),
			},
		}
	case *parser.OrExpr:
		result = &expr{
			op: orOp,
			children: []*expr{
				buildScalar(t.Left, props),
				buildScalar(t.Right, props),
			},
		}
	case *parser.NotExpr:
		result = &expr{
			op: notOp,
			children: []*expr{
				buildScalar(t.Expr, props),
			},
		}

	case *parser.BinaryExpr:
		result = &expr{
			op: binaryOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Left, props),
				buildScalar(t.Right, props),
			},
		}
	case *parser.ComparisonExpr:
		result = &expr{
			op: comparisonOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Left, props),
				buildScalar(t.Right, props),
			},
		}
	case *parser.UnaryExpr:
		result = &expr{
			op: unaryOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Expr, props),
			},
		}

	case *parser.ColumnItem:
		tableName := t.TableName.Table()
		colName := string(t.ColumnName)
		for _, col := range props.columns {
			if col.hasColumn(tableName, colName) {
				if tableName == "" && len(col.tables) > 0 {
					t.TableName.TableName = parser.Name(col.tables[0])
					t.TableName.DBNameOriginallyOmitted = true
				}
				result = &expr{
					op:        variableOp,
					dataIndex: props.state.addData(t),
					props:     props,
				}
				result.setVarIndex(col.index)
				result.inputVars.set(col.index)
				result.updateProps()
				return result
			}
		}
		fatalf("unknown column %s", t)

	case parser.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return buildScalar(vn, props)

	case *parser.NumVal:
		result = &expr{
			op:        constOp,
			dataIndex: props.state.addData(t),
			props:     props,
		}

	case *parser.ExistsExpr:
		result = &expr{
			op: existsOp,
			children: []*expr{
				buildScalar(t.Subquery, props),
			},
		}

	case *parser.Subquery:
		return build(t.Select, props)

	default:
		unimplemented("%T", pexpr)
	}
	result.updateProps()
	return result
}

func buildSelect(stmt *parser.Select, props *logicalProps) *expr {
	var result *expr

	switch t := stmt.Select.(type) {
	case *parser.SelectClause:
		result = buildFrom(t.From, t.Where, props)
		result = buildGroupBy(result, t.GroupBy, t.Having)
		result = buildProjections(result, t.Exprs)
		result = buildDistinct(result, t.Distinct)

	case *parser.UnionClause:
		result = buildUnion(t, props)

	case *parser.ParenSelect:
		result = buildSelect(t.Select, props)

	// TODO(peter): case *parser.ValuesClause:

	default:
		unimplemented("%T", stmt.Select)
	}

	result = buildOrderBy(result, stmt.OrderBy)
	// TODO(peter): stmt.Limit
	return result
}

func buildFrom(from *parser.From, where *parser.Where, props *logicalProps) *expr {
	if from == nil {
		return nil
	}

	var result *expr
	for _, table := range from.Tables {
		t := buildTable(table, props)
		if result == nil {
			result, props = t, t.props
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
		props = result.props
	}

	if where != nil {
		result.addFilter(buildScalar(where.Expr, props))
		result.updateProps()
	}

	return result
}

func buildGroupBy(input *expr, groupBy parser.GroupBy, having *parser.Where) *expr {
	if groupBy == nil {
		return input
	}

	result := &expr{
		op:       groupByOp,
		children: []*expr{input},
		props:    input.props,
	}

	exprs := make([]*expr, 0, len(groupBy))
	for _, expr := range groupBy {
		exprs = append(exprs, buildScalar(expr, result.props))
	}
	result.addGroupings(exprs)

	result.updateProps()

	if having != nil {
		// TODO(peter): Any aggregations mentioned in the having expressions need
		// to be copied into the groupByOp. Ditto for later projections.
		result.addFilter(buildScalar(having.Expr, result.props))
		result.updateProps()
	}

	return result
}

func buildProjection(pexpr parser.Expr, props *logicalProps) []*expr {
	switch t := pexpr.(type) {
	case parser.UnqualifiedStar:
		var projections []*expr
		for _, col := range props.columns {
			projections = append(projections, col.newVariableExpr("", props))
		}
		if len(projections) == 0 {
			fatalf("failed to expand *")
		}
		return projections

	case *parser.AllColumnsSelector:
		tableName := t.TableName.Table()
		var projections []*expr
		for _, col := range props.columns {
			if col.hasTable(tableName) {
				projections = append(projections, col.newVariableExpr(tableName, props))
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
		return buildProjection(vn, props)

	default:
		return []*expr{buildScalar(pexpr, props)}
	}
}

func buildProjections(input *expr, sexprs parser.SelectExprs) *expr {
	if len(sexprs) == 0 {
		return input
	}

	state := input.props.state
	result := &expr{
		op: projectOp,
		children: []*expr{
			input,
		},
		props: &logicalProps{state: state},
	}

	var projections []*expr
	for _, expr := range sexprs {
		exprs := buildProjection(expr.Expr, input.props)
		projections = append(projections, exprs...)

		for _, p := range exprs {
			name := string(expr.As)
			var tables []string

			if !p.hasVarIndex() {
				p.setVarIndex(state.nextVar)
				state.nextVar++
				if name == "" {
					name = fmt.Sprintf("column%d", len(result.props.columns)+1)
				}
			} else {
				for _, col := range input.props.columns {
					if p.varIndex == col.index {
						if name == "" {
							name = col.name
						}
						tables = col.tables
					}
				}
			}

			result.props.columns = append(result.props.columns, columnProps{
				index:  p.varIndex,
				name:   name,
				tables: tables,
			})
		}
	}

	result.addProjections(projections)
	result.updateProps()
	return result
}

func buildDistinct(input *expr, distinct bool) *expr {
	if !distinct {
		return input
	}

	// Distint is equivalent to group by without any aggregations.
	result := &expr{
		op:       groupByOp,
		children: []*expr{input},
		props:    input.props,
	}

	exprs := make([]*expr, 0, len(input.props.columns))
	for _, col := range input.props.columns {
		exprs = append(exprs, col.newVariableExpr("", input.props))
	}
	result.addGroupings(exprs)

	result.updateProps()
	return result
}

func buildOrderBy(input *expr, orderBy parser.OrderBy) *expr {
	if orderBy == nil {
		return input
	}

	// TODO(peter): order by is not a relational expression, but instead a
	// required property on the output.
	result := &expr{
		op:        orderByOp,
		dataIndex: input.props.state.addData(orderBy),
		children:  []*expr{input},
		props:     input.props,
	}
	result.updateProps()
	return result
}

func buildUnion(clause *parser.UnionClause, props *logicalProps) *expr {
	op := unionOp
	switch clause.Type {
	case parser.UnionOp:
	case parser.IntersectOp:
		op = intersectOp
	case parser.ExceptOp:
		op = exceptOp
	}
	left := buildSelect(clause.Left, props)
	right := buildSelect(clause.Right, props)
	result := &expr{
		op: op,
		children: []*expr{
			left,
			right,
		},
		props: left.props,
	}
	result.updateProps()
	return result
}
