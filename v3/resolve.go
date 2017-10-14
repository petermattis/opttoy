package v3

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func resolve(e *expr, state *queryState, parent *expr) {
	for _, input := range e.inputs() {
		resolve(input, state, e)
	}

	resolveBody(e, state, parent)

	for _, filter := range e.filters() {
		resolve(filter, state, e)
	}

	for i := 0; i < len(e.projections()); i++ {
		project := e.projections()[i]
		resolve(project, state, e)
		if project != e.projections()[i] {
			// Resolving the projection caused it to change. Back up and resolve
			// again.
			i--
			continue
		}
		if project.outputVars == 0 {
			project.outputVars.set(bitmapIndex(len(state.columns)))
			state.columns = append(state.columns, columnRef{
				// relation: expr
				index: columnIndex(i),
			})
		}
	}

	e.updateProperties()
}

func resolveBody(e *expr, state *queryState, parent *expr) {
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
		for i := range table.columnNames {
			e.inputVars.set(base + bitmapIndex(i))
		}

	case parser.UnqualifiedStar:
		addColumns := func(projections []*expr, columns []bitmapIndex) []*expr {
			for _, colIndex := range columns {
				col := state.columns[colIndex]
				p := &expr{
					op: variableOp,
					body: parser.UnresolvedName{
						parser.Name(col.table.name),
						parser.Name(col.table.columnNames[col.index]),
					},
				}
				projections = append(projections, p)
			}
			return projections
		}
		var newProjections []*expr
		if inputs := parent.inputs(); len(inputs) == 0 {
			newProjections = addColumns(newProjections, parent.inputVars.indexes())
		} else {
			for _, input := range inputs {
				newProjections = addColumns(newProjections, input.columns())
			}
		}
		parent.replaceProjection(e, newProjections)

	case parser.UnresolvedName:
		if len(b) != 2 {
			fatalf("unsupported unqualified name: %s", b)
		}
		tableName := string(b[0].(parser.Name))
		if base, ok := state.tables[tableName]; !ok {
			fatalf("unknown table %s", b)
		} else if table, ok := state.catalog[tableName]; !ok {
			fatalf("unknown table %s", b)
		} else {
			switch t := b[1].(type) {
			case parser.Name:
				colName := string(t)
				if colIndex, ok := table.columns[colName]; !ok {
					fatalf("unknown column %s", b)
				} else {
					e.inputVars.set(base + bitmapIndex(colIndex))
				}
			case parser.UnqualifiedStar:
				newProjections := make([]*expr, 0, len(table.columnNames))
				for _, colName := range table.columnNames {
					newProjections = append(newProjections, &expr{
						op: variableOp,
						body: parser.UnresolvedName{
							parser.Name(tableName),
							parser.Name(colName),
						},
					})
				}
				parent.replaceProjection(e, newProjections)
			default:
				unimplemented("%T", b[1])
			}
		}

	case *parser.NumVal:

	case parser.NaturalJoinCond:
		resolveNaturalJoin(e, state)

	case *parser.UsingJoinCond:
		resolveUsingJoin(e, state, b.Cols)

	case *parser.ExistsExpr:
		// TODO(peter): unimplemented.

	default:
		unimplemented("%T", e.body)
	}
}

func resolveNaturalJoin(e *expr, state *queryState) {
	common := make(map[string]struct{})
	for i, input := range e.inputs() {
		table := input.body.(*table)
		if i == 0 {
			for col := range table.columns {
				common[col] = struct{}{}
			}
		} else {
			for col := range common {
				if _, ok := table.columns[col]; !ok {
					delete(common, col)
				}
			}
		}
	}

	names := make(parser.NameList, 0, len(common))
	for col := range common {
		names = append(names, parser.Name(col))
	}
	sort.Slice(names, func(i, j int) bool {
		return names[i] < names[j]
	})

	resolveUsingJoin(e, state, names)
}

func resolveUsingJoin(e *expr, state *queryState, names parser.NameList) {
	// TODO(peter): check for validity of the names.

	for _, name := range names {
		// For every adjacent pair of tables, add an equality predicate.
		inputs := e.inputs()
		for i := 1; i < len(inputs); i++ {
			left := inputs[i-1].body.(*table)
			right := inputs[i].body.(*table)
			e.addFilter(&expr{
				op: eqOp,
				children: []*expr{
					{
						op: variableOp,
						body: parser.UnresolvedName{
							parser.Name(left.name),
							name,
						},
					},
					{
						op: variableOp,
						body: parser.UnresolvedName{
							parser.Name(right.name),
							name,
						},
					},
				},
				inputCount: 2,
			})
		}
	}

	e.body = nil
}
