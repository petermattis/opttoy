package v3

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

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

func build(stmt parser.Statement) *expr {
	switch stmt := stmt.(type) {
	case *parser.Select:
		return buildSelect(stmt)
	case *parser.ParenSelect:
		return buildSelect(stmt.Select)
	default:
		unimplemented("%T", stmt)
		return nil
	}
}

func buildTable(table parser.TableExpr) *expr {
	switch source := table.(type) {
	case *parser.NormalizableTableName:
		return &expr{
			op:   scanOp,
			body: source,
		}
	case *parser.AliasedTableExpr:
		// TODO(peter): handle source.As.
		e := buildTable(source.Expr)
		if source.As.Alias != "" {
			e = &expr{
				op:         renameOp,
				children:   []*expr{e},
				inputCount: 1,
				body:       source.As,
			}
		}
		return e
	case *parser.ParenTableExpr:
		return buildTable(source.Expr)
	case *parser.JoinTableExpr:
		result := &expr{
			op: innerJoinOp,
			children: []*expr{
				buildTable(source.Left),
				buildTable(source.Right),
			},
			inputCount: 2,
		}

		switch cond := source.Cond.(type) {
		case *parser.OnJoinCond:
			result.addFilter(buildExpr(cond.Expr))

		case parser.NaturalJoinCond:
			result.body = cond
		case *parser.UsingJoinCond:
			result.body = cond

		default:
			unimplemented("%T", source.Cond)
		}
		return result
	case *parser.Subquery:
		return build(source.Select)

	default:
		unimplemented("%T", table)
		return nil
	}
}

func buildExpr(pexpr parser.Expr) *expr {
	switch t := pexpr.(type) {
	case *parser.ParenExpr:
		return buildExpr(t.Expr)

	case *parser.AndExpr:
		return &expr{
			op: andOp,
			children: []*expr{
				buildExpr(t.Left),
				buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.OrExpr:
		return &expr{
			op: orOp,
			children: []*expr{
				buildExpr(t.Left),
				buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.NotExpr:
		return &expr{
			op: notOp,
			children: []*expr{
				buildExpr(t.Expr),
			},
			inputCount: 1,
		}

	case *parser.BinaryExpr:
		return &expr{
			op: binaryOpMap[t.Operator],
			children: []*expr{
				buildExpr(t.Left),
				buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.ComparisonExpr:
		return &expr{
			op: comparisonOpMap[t.Operator],
			children: []*expr{
				buildExpr(t.Left),
				buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.UnaryExpr:
		return &expr{
			op: unaryOpMap[t.Operator],
			children: []*expr{
				buildExpr(t.Expr),
			},
			inputCount: 1,
		}

	case parser.UnqualifiedStar:
		return &expr{
			op:   variableOp,
			body: t,
		}
	case parser.UnresolvedName:
		return &expr{
			op:   variableOp,
			body: t,
		}
	case *parser.NumVal:
		return &expr{
			op:   constOp,
			body: t,
		}

	case *parser.ExistsExpr:
		return &expr{
			op: existsOp,
			children: []*expr{
				buildExpr(t.Subquery),
			},
			inputCount: 1,
		}

	case *parser.Subquery:
		return build(t.Select)

	default:
		unimplemented("%T", pexpr)
		return nil
	}
}

func buildSelect(stmt *parser.Select) *expr {
	switch t := stmt.Select.(type) {
	case *parser.SelectClause:
		// TODO: stmt.Limit
		return buildSelectClause(t, stmt.OrderBy)
	case *parser.UnionClause:
		return buildUnion(t)
	// TODO: handle other stmt.Select types.
	default:
		unimplemented("%T", stmt.Select)
	}
	return nil
}

func buildSelectClause(clause *parser.SelectClause, orderBy parser.OrderBy) *expr {
	var result *expr
	if clause.From != nil {
		var inputs []*expr
		for _, table := range clause.From.Tables {
			inputs = append(inputs, buildTable(table))
		}
		if len(inputs) == 1 {
			result = inputs[0]
		} else {
			result = &expr{
				op:         innerJoinOp,
				children:   inputs,
				inputCount: int16(len(inputs)),
				body:       parser.NaturalJoinCond{},
			}
		}
		if clause.Where != nil {
			result = &expr{
				op: selectOp,
				children: []*expr{
					result,
				},
				inputCount: 1,
			}
			result.addFilter(buildExpr(clause.Where.Expr))
		}
	}

	if clause.GroupBy != nil {
		result = &expr{
			op:         groupByOp,
			children:   []*expr{result},
			inputCount: 1,
		}
		if clause.Having != nil {
			result = &expr{
				op: selectOp,
				children: []*expr{
					result,
				},
				inputCount: 1,
			}
			result.addFilter(buildExpr(clause.Having.Expr))
		}
	}

	if len(clause.Exprs) > 0 {
		result = &expr{
			op: projectOp,
			children: []*expr{
				result,
			},
			inputCount: 1,
		}
		for _, expr := range clause.Exprs {
			// TODO(peter): handle expr.As
			result.addProjection(buildExpr(expr.Expr))
		}
	}

	if clause.Distinct {
		result = &expr{
			op:         distinctOp,
			children:   []*expr{result},
			inputCount: 1,
		}
	}

	// TODO: order by is not a relational expression, but instead a required
	// property on the output.
	if orderBy != nil {
		result = &expr{
			op:         orderByOp,
			children:   []*expr{result},
			inputCount: 1,
			body:       orderBy,
		}
	}

	return result
}

func buildUnion(clause *parser.UnionClause) *expr {
	op := unionOp
	switch clause.Type {
	case parser.UnionOp:
	case parser.IntersectOp:
		op = intersectOp
	case parser.ExceptOp:
		op = exceptOp
	}
	return &expr{
		op: op,
		children: []*expr{
			buildSelect(clause.Left),
			buildSelect(clause.Right),
		},
		inputCount: 2,
	}
}
