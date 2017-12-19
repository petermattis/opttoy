package v4

import (
	"fmt"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
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

type columnProps struct {
	name  columnName
	table tableName
	index colsetIndex
}

func (c columnProps) String() string {
	if c.table == "" {
		return tree.Name(c.name).String()
	}
	return fmt.Sprintf("%s.%s", tree.Name(c.table), tree.Name(c.name))
}

func (c columnProps) matches(tblName tableName, colName columnName) bool {
	if colName != c.name {
		return false
	}
	if tblName == "" {
		return true
	}
	return c.table == tblName
}

type scope struct {
	parent *scope
	props  *relationalProps
}

func (s *scope) push(props *relationalProps) *scope {
	return &scope{
		parent: s,
		props:  props,
	}
}

type builder struct {
	md      *metadata
	stmt    tree.Statement
	memo    *memo
	factory *factory

	// nextVar keeps track of the next index for a column.
	nextVar colsetIndex
}

func newBuilder(md *metadata, stmt tree.Statement) *builder {
	memo := newMemo()
	return &builder{md: md, stmt: stmt, memo: memo, factory: newFactory(memo)}
}

func (b *builder) build() *memo {
	root := b.buildStmt(b.stmt, &scope{props: &relationalProps{}})
	b.memo.addRoot(root)
	return b.memo
}

func (b *builder) buildStmt(stmt tree.Statement, scope *scope) groupID {
	switch stmt := b.stmt.(type) {
	case *tree.Select:
		return b.buildSelect(stmt, scope)
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, scope)
	default:
		unimplemented("%T", stmt)
		return 0
	}
}

func (b *builder) buildTable(texpr tree.TableExpr, scope *scope) groupID {
	switch source := texpr.(type) {
	case *tree.NormalizableTableName:
		tn, err := source.Normalize()
		if err != nil {
			fatalf("%s", err)
		}

		name := tableName(tn.Table())
		tab, ok := b.md.catalog[name]
		if !ok {
			fatalf("unknown table %s", name)
		}

		return b.buildScan(tab, scope)

	case *tree.AliasedTableExpr:
		return b.buildTable(source.Expr, scope)

	case *tree.ParenTableExpr:
		return b.buildTable(source.Expr, scope)

	case *tree.JoinTableExpr:
		left := b.buildTable(source.Left, scope)
		scope := scope.push(&b.lookupLogicalProps(left).relational)
		right := b.buildTable(source.Right, scope)

		switch cond := source.Cond.(type) {
		case *tree.OnJoinCond:
			filter := b.buildScalar(cond.Expr, scope)
			return b.factory.constructInnerJoin(left, right, filter)

		case tree.NaturalJoinCond:
			b.buildNaturalJoin(left, right)

		case *tree.UsingJoinCond:
			b.buildUsingJoin(left, right, cond.Cols)

		default:
			unimplemented("%T", source.Cond)
		}

		result := newJoinExpr(joinOp(source.Join), left, right)
		result.props = &relationalProps{}

		return result

	case *tree.Subquery:
		return b.buildStmt(source.Select, scope)

	default:
		unimplemented("%T", texpr)
		return 0
	}
}

func (b *builder) buildScan(tab *table, scope *scope) groupID {
	// Every reference to a table in the query gets a new set of output column
	// indexes. Consider the query:
	//
	//   SELECT * FROM a AS l JOIN a AS r ON (l.x = r.y)
	//
	// In this query, `l.x` is not equivalent to `r.x` and `l.y` is not
	// equivalent to `r.y`. In order to achieve this, we need to give these
	// columns different indexes.
	base := b.nextVar
	b.nextVar += colsetIndex(len(tab.columns))

	// TODO(peter): metadata.tables is used for looking up foreign key
	// references. Currently, this lookup is global, but it likely needs to be
	// scoped.
	// TODO(andy): if two instances of the same table need to be treated
	// separately, then why is the table name always mapped to the same base?
	if _, ok := b.md.tables[tab.name]; !ok {
		b.md.tables[tab.name] = base
	}

	return b.factory.constructScan(b.memo.internPrivate(tab))
}

func (b *builder) buildOnJoin(left, right *memoGroup, on tree.Expr, scope *scope) {
	left := result.inputs()[0].props
	right := result.inputs()[1].props
	result.props.columns = make([]columnProps, len(left.columns)+len(right.columns))
	copy(result.props.columns[:], left.columns)
	copy(result.props.columns[len(left.columns):], right.columns)
	result.addFilter(buildScalar(on, scope.push(result.props)))
}

func (b *builder) buildNaturalJoin(left, right groupID) {
	leftProps := &b.lookupLogicalProps(left).relational
	rightProps := &b.lookupLogicalProps(right).relational

	names := make(tree.NameList, 0, len(leftProps.columns))
	for _, col := range leftProps.columns {
		if !col.hidden {
			names = append(names, tree.Name(col.name))
		}
	}

	var common tree.NameList
	for _, colName := range names {
		for _, col := range rightProps.columns {
			if !col.hidden && colName == tree.Name(col.name) {
				common = append(common, colName)
				break
			}
		}
	}

	names = common
	b.buildUsingJoin(e, names)
}

func (b *builder) buildUsingJoin(left, right groupID, names tree.NameList) {
	leftProps := &b.lookupLogicalProps(left).relational
	rightProps := &b.lookupLogicalProps(right).relational

	filter := groupID(0)
	joined := make(map[columnName]*columnProps, len(names))
	for _, name := range names {
		name := columnName(name)

		// For every adjacent pair of tables, add an equality predicate.
		leftCol := leftProps.findColumn(name)
		if leftCol == nil {
			fatalf("unable to resolve name %s", name)
		}

		rightCol := rightProps.findColumn(name)
		if rightCol == nil {
			fatalf("unable to resolve name %s", name)
		}

		leftVar := b.factory.constructVariable(b.memo.internPrivate(leftCol))
		rightVar := b.factory.constructVariable(b.memo.internPrivate(rightCol))
		eq := b.factory.constructEq(leftVar, rightVar)

		if filter == 0 {
			filter = eq
		} else {
			filter = b.factory.constructAnd(eq, filter)
		}

		joined[name] = leftCol
	}

	for _, col := range leftProps.columns {
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

func (b *builder) buildScalar(pexpr tree.Expr, scope *scope) groupID {
	var result *expr
	switch t := pexpr.(type) {
	case *tree.ParenExpr:
		return b.buildScalar(t.Expr, scope)

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
		colName := columnName(t.ColumnName)

		for s := scope; s != nil; s = s.parent {
			for _, col := range s.props.columns {
				if col.matches(tblName, colName) {
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

	case *tree.Placeholder:
		result = &expr{
			op:          placeholderOp,
			scalarProps: &scalarProps{},
			private:     t,
		}

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

func (b *builder) buildSelect(stmt *tree.Select, scope *scope) groupID {
	var result *expr

	switch t := stmt.Select.(type) {
	case *tree.SelectClause:
		result, scope = b.buildFrom(t.From, t.Where, scope)
		result, scope = b.buildGroupBy(result, t.GroupBy, t.Having, scope)
		result, scope = b.buildProjections(result, t.Exprs, scope)
		result, scope = b.buildDistinct(result, t.Distinct, scope)

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

func (b *builder) buildFrom(from *tree.From, where *tree.Where, scope *scope) (groupID, *scope) {
	if from == nil {
		return 0, scope
	}

	var result *expr
	for _, table := range from.Tables {
		t := b.buildTable(table, scope)
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

func (b *builder) buildGroupBy(
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

func (b *builder) buildGroupByExtractAggregates(g *expr, e *expr, scope *scope) bool {
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
		name := columnName(fmt.Sprintf("column%d", len(g.props.columns)+1))
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

func (b *builder) buildProjection(pexpr tree.Expr, scope *scope) []*expr {
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

func (b *builder) buildProjections(
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
				index := scope.state.nextVar
				scope.state.nextVar++
				if name == "" {
					name = columnName(fmt.Sprintf("column%d", len(result.props.columns)+1))
				}
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

func (b *builder) buildDistinct(input *expr, distinct bool, scope *scope) (*expr, *scope) {
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

func (b *builder) buildOrderBy(input *expr, orderBy tree.OrderBy, scope *scope) *expr {
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

func (b *builder) buildUnion(clause *tree.UnionClause, scope *scope) *expr {
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

func (b *builder) lookupLogicalProps(group groupID) *logicalProps {
	return b.memo.lookupLogicalProps(b.memo.lookupGroup(group).logical)
}

func joinOp(s string) operator {
	switch s {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		return innerJoinOp
	case "LEFT JOIN":
		return leftJoinOp
	case "RIGHT JOIN":
		return rightJoinOp
	case "FULL JOIN":
		return fullJoinOp
	default:
		unimplemented("unsupported JOIN type %s", s)
		return unknownOp
	}
}
