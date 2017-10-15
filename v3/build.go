package v3

import (
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

type columnInfo struct {
	index  bitmapIndex
	name   string
	tables []string
}

func (c columnInfo) hasColumn(tableName, colName string) bool {
	if colName != c.name {
		return false
	}
	if tableName == "" {
		return true
	}
	return c.hasTable(tableName)
}

func (c columnInfo) hasTable(tableName string) bool {
	for _, t := range c.tables {
		if t == tableName {
			return true
		}
	}
	return false
}

func (c columnInfo) resolvedName(tableName string) *parser.ColumnItem {
	if tableName == "" {
		if len(c.tables) > 0 {
			tableName = c.tables[0]
		}
	}
	return &parser.ColumnItem{
		TableName: parser.TableName{
			TableName:               parser.Name(tableName),
			DBNameOriginallyOmitted: true,
		},
		ColumnName: parser.Name(c.name),
	}
}

func (c columnInfo) newVariableExpr(tableName string) *expr {
	e := &expr{
		op:   variableOp,
		body: c.resolvedName(tableName),
	}
	e.inputVars.set(c.index)
	e.updateProperties()
	return e
}

func findColumnInfo(cols []columnInfo, name string) columnInfo {
	for _, col := range cols {
		if col.name == name {
			return col
		}
	}
	return columnInfo{}
}

func concatColumns(cols [][]columnInfo) []columnInfo {
	if len(cols) == 1 {
		return cols[0]
	}
	var res []columnInfo
	for _, c := range cols {
		res = append(res, c...)
	}
	return res
}

func build(
	stmt parser.Statement,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	switch stmt := stmt.(type) {
	case *parser.Select:
		return buildSelect(stmt, state, inputCols)
	case *parser.ParenSelect:
		return buildSelect(stmt.Select, state, inputCols)
	default:
		unimplemented("%T", stmt)
		return nil, nil
	}
}

func buildTable(
	table parser.TableExpr,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	switch source := table.(type) {
	case *parser.NormalizableTableName:
		e := &expr{
			op:   scanOp,
			body: source,
		}
		tableName, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}
		name := tableName.Table()
		table, ok := state.catalog[name]
		if !ok {
			fatalf("unknown table %s", name)
		}
		e.body = table

		base, ok := state.tables[name]
		if !ok {
			base = bitmapIndex(len(state.columns))
			state.tables[name] = base
			for i := range table.columnNames {
				state.columns = append(state.columns, columnRef{
					table: table,
					index: columnIndex(i),
				})
			}
		}
		cols := make([]columnInfo, 0, len(table.columnNames))
		for i, colName := range table.columnNames {
			index := base + bitmapIndex(i)
			e.inputVars.set(index)
			cols = append(cols, columnInfo{
				index:  index,
				name:   colName,
				tables: []string{table.name},
			})
		}
		e.updateProperties()
		return e, cols

	case *parser.AliasedTableExpr:
		result, cols := buildTable(source.Expr, state, inputCols)
		if source.As.Alias != "" {
			if n := len(source.As.Cols); n > 0 && n != len(cols) {
				fatalf("rename specified %d columns, but table contains %d",
					n, len(cols))
			}

			result = &expr{
				op:         renameOp,
				children:   []*expr{result},
				inputCount: 1,
				body:       source.As,
			}

			newCols := make([]columnInfo, 0, len(cols))
			for i, col := range cols {
				name := col.name
				if i < len(source.As.Cols) {
					name = string(source.As.Cols[i])
				}

				newCols = append(newCols, columnInfo{
					index:  col.index,
					name:   name,
					tables: []string{string(source.As.Alias)},
				})
			}

			result.updateProperties()
			return result, newCols
		}
		return result, cols

	case *parser.ParenTableExpr:
		return buildTable(source.Expr, state, inputCols)

	case *parser.JoinTableExpr:
		left, leftCols := buildTable(source.Left, state, inputCols)
		right, rightCols := buildTable(source.Right, state, inputCols)
		result := &expr{
			op: innerJoinOp,
			children: []*expr{
				left,
				right,
			},
			inputCount: 2,
		}

		inputCols := [][]columnInfo{leftCols, rightCols}
		var cols []columnInfo

		switch cond := source.Cond.(type) {
		case *parser.OnJoinCond:
			cols = concatColumns(inputCols)
			result.addFilter(buildScalar(cond.Expr, state, cols))

		case parser.NaturalJoinCond:
			cols = buildNaturalJoin(result, state, inputCols)

		case *parser.UsingJoinCond:
			cols = buildUsingJoin(result, state, cond.Cols, inputCols)

		default:
			unimplemented("%T", source.Cond)
		}

		result.updateProperties()
		return result, cols

	case *parser.Subquery:
		return build(source.Select, state, inputCols)

	default:
		unimplemented("%T", table)
		return nil, nil
	}
}

func buildNaturalJoin(
	e *expr,
	state *queryState,
	inputCols [][]columnInfo,
) []columnInfo {
	names := make(parser.NameList, 0, len(inputCols[0]))
	for _, col := range inputCols[0] {
		names = append(names, parser.Name(col.name))
	}
	for _, columns := range inputCols[1:] {
		var common parser.NameList
		for _, colName := range names {
			for _, col := range columns {
				if colName == parser.Name(col.name) {
					common = append(common, colName)
				}
			}
		}
		names = common
	}
	return buildUsingJoin(e, state, names, inputCols)
}

func buildUsingJoin(
	e *expr,
	state *queryState,
	names parser.NameList,
	inputCols [][]columnInfo,
) []columnInfo {
	e.body = nil

	joined := make(map[string]int, len(names))
	for _, name := range names {
		joined[string(name)] = -1
		// For every adjacent pair of tables, add an equality predicate.
		for i := 1; i < len(inputCols); i++ {
			left := findColumnInfo(inputCols[i-1], string(name))
			if left.tables == nil {
				fatalf("unable to resolve name %s", name)
			}
			right := findColumnInfo(inputCols[i], string(name))
			if right.tables == nil {
				fatalf("unable to resolve name %s", name)
			}
			f := &expr{
				op: eqOp,
				children: []*expr{
					left.newVariableExpr(""),
					right.newVariableExpr(""),
				},
				inputCount: 2,
			}
			f.updateProperties()
			e.addFilter(f)
		}
	}

	var res []columnInfo
	for _, columns := range inputCols {
		for _, col := range columns {
			if idx, ok := joined[col.name]; ok {
				if idx != -1 {
					oldCol := res[idx]
					res[idx] = columnInfo{
						index:  oldCol.index,
						name:   oldCol.name,
						tables: append(oldCol.tables, col.tables[0]),
					}
					continue
				}
				joined[col.name] = len(res)
			}

			res = append(res, columnInfo{
				index:  col.index,
				name:   col.name,
				tables: []string{col.tables[0]},
			})
		}
	}
	return res
}

func buildScalar(
	pexpr parser.Expr,
	state *queryState,
	inputCols []columnInfo,
) *expr {
	var result *expr
	switch t := pexpr.(type) {
	case *parser.ParenExpr:
		return buildScalar(t.Expr, state, inputCols)

	case *parser.AndExpr:
		result = &expr{
			op: andOp,
			children: []*expr{
				buildScalar(t.Left, state, inputCols),
				buildScalar(t.Right, state, inputCols),
			},
			inputCount: 2,
		}
	case *parser.OrExpr:
		result = &expr{
			op: orOp,
			children: []*expr{
				buildScalar(t.Left, state, inputCols),
				buildScalar(t.Right, state, inputCols),
			},
			inputCount: 2,
		}
	case *parser.NotExpr:
		result = &expr{
			op: notOp,
			children: []*expr{
				buildScalar(t.Expr, state, inputCols),
			},
			inputCount: 1,
		}

	case *parser.BinaryExpr:
		result = &expr{
			op: binaryOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Left, state, inputCols),
				buildScalar(t.Right, state, inputCols),
			},
			inputCount: 2,
		}
	case *parser.ComparisonExpr:
		result = &expr{
			op: comparisonOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Left, state, inputCols),
				buildScalar(t.Right, state, inputCols),
			},
			inputCount: 2,
		}
	case *parser.UnaryExpr:
		result = &expr{
			op: unaryOpMap[t.Operator],
			children: []*expr{
				buildScalar(t.Expr, state, inputCols),
			},
			inputCount: 1,
		}

	case *parser.ColumnItem:
		tableName := t.TableName.Table()
		colName := string(t.ColumnName)
		for _, col := range inputCols {
			if col.hasColumn(tableName, colName) {
				if tableName == "" && len(col.tables) > 0 {
					t.TableName.TableName = parser.Name(col.tables[0])
					t.TableName.DBNameOriginallyOmitted = true
				}
				result = &expr{
					op:   variableOp,
					body: t,
				}
				result.inputVars.set(col.index)
				result.updateProperties()
				return result
			}
		}
		fatalf("unknown column %s", t)

	case parser.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return buildScalar(vn, state, inputCols)

	case *parser.NumVal:
		result = &expr{
			op:   constOp,
			body: t,
		}

	case *parser.ExistsExpr:
		result = &expr{
			op: existsOp,
			children: []*expr{
				buildScalar(t.Subquery, state, inputCols),
			},
			inputCount: 1,
		}

	case *parser.Subquery:
		result, _ := build(t.Select, state, inputCols)
		return result

	default:
		unimplemented("%T", pexpr)
	}
	result.updateProperties()
	return result
}

func buildSelect(
	stmt *parser.Select,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	var result *expr
	var cols []columnInfo

	switch t := stmt.Select.(type) {
	case *parser.SelectClause:
		result, cols = buildFrom(t.From, t.Where, state, inputCols)
		result, cols = buildGroupBy(result, t.GroupBy, t.Having, state, cols)
		result, cols = buildProjections(result, t.Exprs, state, cols)
		result, cols = buildDistinct(result, t.Distinct, state, cols)

	case *parser.UnionClause:
		result, cols = buildUnion(t, state, inputCols)

	case *parser.ParenSelect:
		result, cols = buildSelect(t.Select, state, inputCols)

	// TODO(peter): case *parser.ValuesClause:

	default:
		unimplemented("%T", stmt.Select)
	}

	result, cols = buildOrderBy(result, stmt.OrderBy, state, cols)
	// TODO(peter): stmt.Limit
	return result, cols
}

func buildFrom(
	from *parser.From,
	where *parser.Where,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	if from == nil {
		return nil, nil
	}

	var result *expr
	for _, table := range from.Tables {
		t, tcols := buildTable(table, state, inputCols)
		if result == nil {
			result, inputCols = t, tcols
			continue
		}
		result = &expr{
			op: innerJoinOp,
			children: []*expr{
				result,
				t,
			},
			inputCount: 2,
		}
		result.updateProperties()
		inputCols = buildNaturalJoin(result, state, [][]columnInfo{inputCols, tcols})
	}

	if where != nil {
		result = &expr{
			op: selectOp,
			children: []*expr{
				result,
			},
			inputCount: 1,
		}
		result.addFilter(buildScalar(where.Expr, state, inputCols))
		result.updateProperties()
	}

	return result, inputCols
}

func buildGroupBy(
	input *expr,
	groupBy parser.GroupBy,
	having *parser.Where,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	if groupBy == nil {
		return input, inputCols
	}

	result := &expr{
		op:         groupByOp,
		children:   []*expr{input},
		inputCount: 1,
	}
	result.updateProperties()

	if having != nil {
		result = &expr{
			op: selectOp,
			children: []*expr{
				result,
			},
			inputCount: 1,
		}
		result.addFilter(buildScalar(having.Expr, state, inputCols))
		result.updateProperties()
	}

	return result, inputCols
}

func buildProjection(
	pexpr parser.Expr,
	state *queryState,
	inputCols []columnInfo,
) []*expr {
	switch t := pexpr.(type) {
	case parser.UnqualifiedStar:
		var projections []*expr
		for _, col := range inputCols {
			projections = append(projections, col.newVariableExpr(""))
		}
		if len(projections) == 0 {
			fatalf("failed to expand *")
		}
		return projections

	case *parser.AllColumnsSelector:
		tableName := t.TableName.Table()
		var projections []*expr
		for _, col := range inputCols {
			if col.hasTable(tableName) {
				projections = append(projections, col.newVariableExpr(tableName))
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
		return buildProjection(vn, state, inputCols)

	default:
		return []*expr{buildScalar(pexpr, state, inputCols)}
	}
}

func buildProjections(
	input *expr,
	exprs parser.SelectExprs,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	if len(exprs) == 0 {
		return input, inputCols
	}

	result := &expr{
		op: projectOp,
		children: []*expr{
			input,
		},
		inputCount: 1,
	}
	var resultCols []columnInfo

	for _, expr := range exprs {
		projections := buildProjection(expr.Expr, state, inputCols)
		for _, p := range projections {
			if p.outputVars == 0 {
				index := bitmapIndex(len(state.columns))
				p.outputVars.set(index)
				state.columns = append(state.columns, columnRef{
					index: columnIndex(result.projectCount),
				})
				name := string(expr.As)
				if name == "" {
					name = expr.Expr.String()
				}
				resultCols = append(resultCols, columnInfo{
					index:  index,
					name:   name,
					tables: []string{},
				})
			} else {
				for _, col := range inputCols {
					if p.outputVars == (bitmap(1) << col.index) {
						name := string(expr.As)
						if name == "" {
							name = col.name
						}
						resultCols = append(resultCols, columnInfo{
							index:  col.index,
							name:   name,
							tables: col.tables,
						})
						break
					}
				}
			}
			result.addProjection(p)
		}
	}
	result.updateProperties()
	return result, resultCols
}

func buildDistinct(
	input *expr,
	distinct bool,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	if !distinct {
		return input, inputCols
	}

	result := &expr{
		op:         distinctOp,
		children:   []*expr{input},
		inputCount: 1,
	}
	result.updateProperties()
	return result, inputCols
}

func buildOrderBy(
	input *expr,
	orderBy parser.OrderBy,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	if orderBy == nil {
		return input, inputCols
	}

	// TODO(peter): order by is not a relational expression, but instead a
	// required property on the output.
	result := &expr{
		op:         orderByOp,
		children:   []*expr{input},
		inputCount: 1,
		body:       orderBy,
	}
	result.updateProperties()
	return result, inputCols
}

func buildUnion(
	clause *parser.UnionClause,
	state *queryState,
	inputCols []columnInfo,
) (*expr, []columnInfo) {
	op := unionOp
	switch clause.Type {
	case parser.UnionOp:
	case parser.IntersectOp:
		op = intersectOp
	case parser.ExceptOp:
		op = exceptOp
	}
	left, leftCols := buildSelect(clause.Left, state, inputCols)
	right, _ := buildSelect(clause.Right, state, inputCols)
	result := &expr{
		op: op,
		children: []*expr{
			left,
			right,
		},
		inputCount: 2,
	}
	result.updateProperties()
	return result, leftCols
}
