package v3

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

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
	return &expr{
		op:   variableOp,
		body: c.resolvedName(tableName),
	}
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

func resolve(e *expr, state *queryState) []columnInfo {
	inputCols := make([][]columnInfo, len(e.inputs()))
	for i, input := range e.inputs() {
		inputCols[i] = resolve(input, state)
	}

	cols := resolveRelationalBody(e, state, inputCols)

	if filters := e.filters(); len(filters) > 0 {
		allInputs := concatColumns(inputCols)
		for _, filter := range filters {
			resolveScalar(filter, state, allInputs)
		}
	}

	if len(e.projections()) > 0 {
		cols = resolveProjections(e, state, concatColumns(inputCols))
	}

	e.updateProperties()
	return cols
}

func resolveProjections(e *expr, state *queryState, inputCols []columnInfo) []columnInfo {
	var cols []columnInfo
	for i := 0; i < len(e.projections()); i++ {
		project := e.projections()[i]
		replacement := resolveScalar(project, state, inputCols)
		if replacement != nil {
			// Resolving the projection caused it to expand. Back up and resolve
			// again.
			e.replaceProjection(project, replacement)
			i--
			continue
		}
		if project.outputVars == 0 {
			index := bitmapIndex(len(state.columns))
			project.outputVars.set(index)
			state.columns = append(state.columns, columnRef{
				index: columnIndex(i),
			})
			// TODO(peter): format the expression to use as the name.
			cols = append(cols, columnInfo{
				index:  index,
				name:   fmt.Sprintf("column%d", i+1),
				tables: []string{},
			})
		} else {
			for _, col := range inputCols {
				if project.outputVars == (bitmap(1) << col.index) {
					cols = append(cols, col)
					break
				}
			}
		}
	}
	return cols
}

func resolveRelationalBody(e *expr, state *queryState, inputCols [][]columnInfo) []columnInfo {
	switch b := e.body.(type) {
	case nil:

	case *parser.NormalizableTableName:
		tableName, err := b.Normalize()
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
		return cols

	case parser.NaturalJoinCond:
		return resolveNaturalJoin(e, state, inputCols)

	case *parser.UsingJoinCond:
		return resolveUsingJoin(e, state, b.Cols, inputCols)

	case parser.AliasClause:
		if len(b.Cols) == 0 {
			allColumns := concatColumns(inputCols)
			cols := make([]columnInfo, 0, len(allColumns))
			for _, col := range allColumns {
				cols = append(cols, columnInfo{
					index:  col.index,
					name:   col.name,
					tables: []string{string(b.Alias)},
				})
			}
			return cols
		}
		// TODO(peter): handle renaming columns as well.
		unimplemented("alias: %s", b)

	default:
		unimplemented("%T", e.body)
	}

	return concatColumns(inputCols)
}

func resolveNaturalJoin(e *expr, state *queryState, inputCols [][]columnInfo) []columnInfo {
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
	return resolveUsingJoin(e, state, names, inputCols)
}

func resolveUsingJoin(e *expr, state *queryState, names parser.NameList, inputCols [][]columnInfo) []columnInfo {
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
			e.addFilter(&expr{
				op: eqOp,
				children: []*expr{
					left.newVariableExpr(""),
					right.newVariableExpr(""),
				},
				inputCount: 2,
			})
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

func resolveScalar(e *expr, state *queryState, cols []columnInfo) []*expr {
	for _, input := range e.inputs() {
		// TODO(peter): This probably does the wrong thing for subqueries.
		resolveScalar(input, state, cols)
	}

	res := resolveScalarBody(e, state, cols)

	// NB: Scalars do not have any filters or projections.

	e.updateProperties()
	return res
}

func resolveScalarBody(e *expr, state *queryState, cols []columnInfo) []*expr {
	switch b := e.body.(type) {
	case nil:

	case parser.UnqualifiedStar:
		var newProjections []*expr
		for _, col := range cols {
			newProjections = append(newProjections, col.newVariableExpr(""))
		}
		if len(newProjections) == 0 {
			fatalf("failed to expand *")
		}
		return newProjections

	case parser.UnresolvedName:
		vn, err := b.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		e.body = vn
		return resolveScalarBody(e, state, cols)

	case *parser.ColumnItem:
		tableName := b.TableName.Table()
		colName := string(b.ColumnName)
		for _, col := range cols {
			if col.hasColumn(tableName, colName) {
				if tableName == "" && len(col.tables) > 0 {
					b.TableName.TableName = parser.Name(col.tables[0])
					b.TableName.DBNameOriginallyOmitted = true
				}
				e.inputVars.set(col.index)
				return nil
			}
		}
		fatalf("unknown column %s", b)

	case *parser.AllColumnsSelector:
		tableName := b.TableName.Table()
		var newProjections []*expr
		for _, col := range cols {
			if col.hasTable(tableName) {
				newProjections = append(newProjections, col.newVariableExpr(tableName))
			}
		}
		if len(newProjections) == 0 {
			fatalf("unknown table %s", b)
		}
		return newProjections

	case *parser.NumVal:

	case *parser.ExistsExpr:
		// TODO(peter): unimplemented.

	default:
		unimplemented("%T", e.body)
	}
	return nil
}
