package opttoy

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

func unimplemented(format string, args ...interface{}) {
	panic("unimplemented: " + fmt.Sprintf(format, args...))
}

func fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// Bitmap used for columns. We're limited to using 64 in a query due to
// laziness. Use FastIntSet in a real implementation.
type bitmap uint64
type bitmapIndex uint

func (b bitmap) String() string {
	appendBitmapRange := func(buf *bytes.Buffer, start, end int) {
		if buf.Len() > 0 {
			fmt.Fprintf(buf, ",")
		}
		if start == end {
			fmt.Fprintf(buf, "%d", start)
		} else {
			fmt.Fprintf(buf, "%d-%d", start, end)
		}
	}

	var buf bytes.Buffer
	start := -1
	for i := 0; i < 64; i++ {
		if b.get(bitmapIndex(i)) {
			if start == -1 {
				start = i
			}
		} else if start != -1 {
			appendBitmapRange(&buf, start, i-1)
			start = -1
		}
	}
	if start != -1 {
		appendBitmapRange(&buf, start, 63)
	}
	return buf.String()
}

func (b bitmap) get(i bitmapIndex) bool {
	return b&(1<<uint(i)) != 0
}

func (b *bitmap) set(i bitmapIndex) {
	*b |= 1 << i
}

type operator int16

const (
	unknownOp operator = iota

	scanOp

	innerJoinOp
	leftJoinOp
	rightJoinOp
	crossJoinOp
	semiJoinOp
	antiJoinOp

	groupByOp
	orderByOp
	distinctOp

	variableOp
	constOp

	existsOp

	andOp
	orOp
	notOp

	eqOp
	ltOp
	gtOp
	leOp
	geOp
	neOp
	inOp
	notInOp
	likeOp
	notLikeOp
	iLikeOp
	notILikeOp
	similarToOp
	notSimilarToOp
	regMatchOp
	notRegMatchOp
	regIMatchOp
	notRegIMatchOp
	isDistinctFromOp
	isNotDistinctFromOp
	isOp
	isNotOp
	anyOp
	someOp
	allOp

	bitandOp
	bitorOp
	bitxorOp
	plusOp
	minusOp
	multOp
	divOp
	floorDivOp
	modOp
	powOp
	concatOp
	lShiftOp
	rShiftOp

	unaryPlusOp
	unaryMinusOp
	unaryComplementOp
)

var operatorName = [...]string{
	unknownOp:           "unknown",
	scanOp:              "scan",
	innerJoinOp:         "inner join",
	leftJoinOp:          "left join",
	rightJoinOp:         "right join",
	crossJoinOp:         "cross join",
	semiJoinOp:          "semi join",
	antiJoinOp:          "anti join",
	groupByOp:           "groupBy",
	orderByOp:           "orderBy",
	distinctOp:          "distinct",
	variableOp:          "variable",
	constOp:             "const",
	existsOp:            "exists",
	andOp:               "logical (AND)",
	orOp:                "logical (OR)",
	notOp:               "logical (NOT)",
	eqOp:                "comp (=)",
	ltOp:                "comp (<)",
	gtOp:                "comp (>)",
	leOp:                "comp (<=)",
	geOp:                "comp (>=)",
	neOp:                "comp (!=)",
	inOp:                "comp (IN)",
	notInOp:             "comp (NOT IN)",
	likeOp:              "comp (LIKE)",
	notLikeOp:           "comp (NOT LIKE)",
	iLikeOp:             "comp (ILIKE)",
	notILikeOp:          "comp (NOT ILIKE)",
	similarToOp:         "comp (SIMILAR TO)",
	notSimilarToOp:      "comp (NOT SIMILAR TO)",
	regMatchOp:          "comp (~)",
	notRegMatchOp:       "comp (!~)",
	regIMatchOp:         "comp (~*)",
	notRegIMatchOp:      "comp (!~*)",
	isDistinctFromOp:    "comp (IS DISTINCT FROM)",
	isNotDistinctFromOp: "comp (IS NOT DISTINCT FROM)",
	isOp:                "comp (IS)",
	isNotOp:             "comp (IS NOT)",
	anyOp:               "comp (ANY)",
	someOp:              "comp (SOME)",
	allOp:               "comp (ALL)",
	bitandOp:            "binary (&)",
	bitorOp:             "binary (|)",
	bitxorOp:            "binary (#)",
	plusOp:              "binary (+)",
	minusOp:             "binary (-)",
	multOp:              "binary (*)",
	divOp:               "binary (/)",
	floorDivOp:          "binary (//)",
	modOp:               "binary (%)",
	powOp:               "binary (^)",
	concatOp:            "binary (||)",
	lShiftOp:            "binary (<<)",
	rShiftOp:            "binary (>>)",
	unaryPlusOp:         "unary (+)",
	unaryMinusOp:        "unary (-)",
	unaryComplementOp:   "unary (~)",
}

func (o operator) String() string {
	if o < 0 || o > operator(len(operatorName)-1) {
		return fmt.Sprintf("operator(%d)", o)
	}
	return operatorName[o]
}

type operatorType int

const (
	unknownType operatorType = iota
	relational
	variable
	scalar
)

var operatorTypeMap = [...]operatorType{
	unknownOp:           unknownType,
	scanOp:              relational,
	innerJoinOp:         relational,
	leftJoinOp:          relational,
	rightJoinOp:         relational,
	crossJoinOp:         relational,
	semiJoinOp:          relational,
	antiJoinOp:          relational,
	groupByOp:           relational,
	orderByOp:           relational,
	distinctOp:          relational,
	variableOp:          variable,
	constOp:             scalar,
	existsOp:            scalar,
	andOp:               scalar,
	orOp:                scalar,
	notOp:               scalar,
	eqOp:                scalar,
	ltOp:                scalar,
	gtOp:                scalar,
	leOp:                scalar,
	geOp:                scalar,
	neOp:                scalar,
	inOp:                scalar,
	notInOp:             scalar,
	likeOp:              scalar,
	notLikeOp:           scalar,
	iLikeOp:             scalar,
	notILikeOp:          scalar,
	similarToOp:         scalar,
	notSimilarToOp:      scalar,
	regMatchOp:          scalar,
	notRegMatchOp:       scalar,
	regIMatchOp:         scalar,
	notRegIMatchOp:      scalar,
	isDistinctFromOp:    scalar,
	isNotDistinctFromOp: scalar,
	isOp:                scalar,
	isNotOp:             scalar,
	anyOp:               scalar,
	someOp:              scalar,
	allOp:               scalar,
	bitandOp:            scalar,
	bitorOp:             scalar,
	bitxorOp:            scalar,
	plusOp:              scalar,
	minusOp:             scalar,
	multOp:              scalar,
	divOp:               scalar,
	floorDivOp:          scalar,
	modOp:               scalar,
	powOp:               scalar,
	concatOp:            scalar,
	lShiftOp:            scalar,
	rShiftOp:            scalar,
	unaryPlusOp:         scalar,
	unaryMinusOp:        scalar,
	unaryComplementOp:   scalar,
}

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

// expr is a unified interface for both relational and scalar expressions in a
// query. Expressions have optional inputs, projections and
// filters. Additionally, an expression maintains a bitmap of required input
// variables and a bitmap of the output variables it generates.
//
// Every unique column and every projection (that is more than just a pass
// through of a variable) is given a variable index with the query. The
// variable indexes are global to the query (see queryState) making the bitmaps
// easily comparable. For example, consider the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 variables in the above query: x and y. During name resolution,
// the above query becomes:
//
//   SELECT @0 FROM a WHERE @1 > 0
//   -- @0 -> x
//   -- @1 -> y
//
// This is akin to the way parser.IndexedVar works except that we're taking
// care to make the indexes unique across the entire statement. Because each of
// the relational expression nodes maintains a bitmap of the variables it
// outputs we can quickly determine if a scalar expression can be handled using
// bitmap intersection.
//
// For scalar expressions the input variables bitmap allows an easy
// determination of whether the expression is constant (the bitmap is empty)
// and, if not, which variables it uses. Predicate push down can use this
// bitmap to quickly determine whether a filter can be pushed below a
// relational operator.
//
// TODO(peter,knz): The bitmap determines which variables are used at each
// level of a logical plan but it does not determine the order in which the
// values are presented in memory, e.g. as a result row. For this each
// expression must also carry, next to the bitmap, an (optional) reordering
// array which maps the positions in the result row to the indexes in the
// bitmap. For example, the two queries SELECT k,v FROM kv and SELECT v,k FROM
// kv have the same bitmap, but the first reorders @1 -> idx 0, @2 -> idx 1
// whereas the second reorders @1 -> idx 1, @2 -> idx 0. More exploration is
// required to determine whether a single array is sufficient for this (indexed
// by the output column position) or whether both backward and forward
// associations must be maintained. Or perhaps some other representation
// altogether.
//
// Relational expressions are composed of inputs, filters and projections. If
// the projections are empty then the input variables flow directly to the
// output after being filtered either by filters or the operation itself
// (e.g. distinctOp). The projections and filters have required variables as
// inputs. From the diagram below, these variables need to either be provided
// by the inputs or be computed by the operator based on the inputs.
//
//   +---------+---------+-------+--------+
//   |  out 0  |  out 1  |  ...  |  out N |
//   +---------+---------+-------+--------+
//   |                |                   |
//   |           projections              |
//   |                |                   |
//   +------------------------------------+
//   |                |                   |
//   |             filters                |
//   |                |                   |
//   +------------------------------------+
//   |             operator               |
//   +---------+---------+-------+--------+
//   |  in 0   |  in 1   |  ...  |  in N  |
//   +---------+---------+-------+--------+
//
// A query is composed of a tree of relational expressions. For example, a
// simple join might look like:
//
//   +-----------+
//   | join a, b |
//   +-----------+
//      |     |
//      |     |   +--------+
//      |     +---| scan b |
//      |         +--------+
//      |
//      |    +--------+
//      +----| scan a |
//           +--------+
//
// The output variables of each expression need to be compatible with input
// columns of its parent expression. And notice that the input variables of an
// expression constrain what output variables we need from the children. That
// constrain can be expressed by bitmap intersection. For example, consider the
// query:
//
//   SELECT a.x FROM a JOIN b USING (x)
//
// The only column from "a" that is required is "x". This is expressed in the
// code by the inputs required by the projection ("a.x") and the inputs
// required by the join condition (also "a.x").
type expr struct {
	// NB: op, inputCount and projectCount are placed next to each other in order
	// to reduce space wastage due to padding.
	op operator
	// The inputs, projections and filters are all stored in the children slice
	// to minimize overhead. The inputCount and projectCount values delineate the
	// input, projection and filter sub-slices:
	//   inputs:      children[:inputCount]
	//   projections: children[inputCount:inputCount+projectCount]
	//   filters:     children[inputCount+projectCount:]
	inputCount   int16
	projectCount int16
	// The input and output bitmaps specified required inputs and generated
	// outputs. The indexes refer to queryState.columns which is constructed on a
	// per-query basis by the columns required by filters, join conditions, and
	// projections and the new columns generated by projections.
	inputVars  bitmap
	outputVars bitmap
	children   []*expr
	// body hold additional info from the AST such as constant values and
	// table/variable names.
	body interface{}
}

func (e *expr) String() string {
	var format func(e *expr, buf *bytes.Buffer, indent string)
	format = func(e *expr, buf *bytes.Buffer, indent string) {
		fmt.Fprintf(buf, "%s%v", indent, e.op)
		if e.body != nil {
			fmt.Fprintf(buf, " (%s)", e.body)
		}
		if e.inputVars != 0 || e.outputVars != 0 {
			fmt.Fprintf(buf, " [")
			sep := ""
			if e.inputVars != 0 {
				fmt.Fprintf(buf, "in=%s", e.inputVars)
				sep = " "
			}
			if e.outputVars != 0 {
				fmt.Fprintf(buf, "%sout=%s", sep, e.outputVars)
			}
			fmt.Fprintf(buf, "]")
		}
		fmt.Fprintf(buf, "\n")
		if projections := e.projections(); len(projections) > 0 {
			fmt.Fprintf(buf, "%s  project:\n", indent)
			for _, project := range projections {
				format(project, buf, indent+"    ")
			}
		}
		if filters := e.filters(); len(filters) > 0 {
			fmt.Fprintf(buf, "%s  filter:\n", indent)
			for _, filter := range filters {
				format(filter, buf, indent+"    ")
			}
		}
		if inputs := e.inputs(); len(inputs) > 0 {
			fmt.Fprintf(buf, "%s  inputs:\n", indent)
			for _, input := range inputs {
				format(input, buf, indent+"    ")
			}
		}
	}

	var buf bytes.Buffer
	format(e, &buf, "")
	return buf.String()
}

func (e *expr) inputs() []*expr {
	return e.children[:e.inputCount]
}

func (e *expr) projections() []*expr {
	return e.children[e.inputCount : e.inputCount+e.projectCount]
}

func (e *expr) addProjection(p *expr) {
	filterStart := e.inputCount + e.projectCount
	e.children = append(e.children, nil)
	copy(e.children[filterStart+1:], e.children[filterStart:])
	e.children[filterStart] = p
	e.projectCount++
}

func (e *expr) removeProjections() {
	if e.projectCount > 0 {
		copy(e.children[e.inputCount:], e.children[e.inputCount+e.projectCount:])
		e.children = e.children[:len(e.children)-int(e.projectCount)]
		e.projectCount = 0
	}
}

func (e *expr) filters() []*expr {
	return e.children[e.inputCount+e.projectCount:]
}

func (e *expr) addFilter(f *expr) {
	// Recursively flatten AND expressions when adding them as a filter. The
	// filters for an expression are implicitly AND'ed together (i.e. they are in
	// conjunctive normal form).
	if f.op == andOp {
		for _, input := range f.inputs() {
			e.addFilter(input)
		}
		return
	}
	e.children = append(e.children, f)
}

func (e *expr) removeFilters() {
	filterStart := e.inputCount + e.projectCount
	e.children = e.children[:int(filterStart)]
}

func applyProjections(expr *expr) {
	expr.outputVars = expr.inputVars
	if projections := expr.projections(); len(projections) > 0 {
		// Restrict output variables based on projections.
		var b bitmap
		for _, project := range projections {
			b |= project.outputVars
		}
		expr.outputVars = b
	}
}

func updateRelationProperties(expr *expr) {
	// Relation operators (join, groupBy, orderBy, distinct) have their required
	// inputs defined by the required inputs of their projections and filters.
	if expr.op != scanOp {
		expr.inputVars = 0
		for _, project := range expr.projections() {
			expr.inputVars |= project.inputVars
		}
		for _, filter := range expr.filters() {
			expr.inputVars |= filter.inputVars
		}
	}

	// Trim the outputs of our inputs based on our requirements.
	for _, input := range expr.inputs() {
		input.outputVars &= expr.inputVars
	}

	// By default the output variables are the input variables. Any projections
	// will override the output variables.
	applyProjections(expr)
}

func updateScalarProperties(expr *expr) {
	// For a scalar operation the required input variables is the union of the
	// required input variables of its inputs. There are no output variables.
	expr.inputVars = 0
	expr.outputVars = 0
	for _, input := range expr.inputs() {
		expr.inputVars |= input.inputVars
	}
}

func updateVariableProperties(expr *expr) {
	// Variables are "pass through": the output variables are the same as the
	// input variables.
	expr.outputVars = expr.inputVars
}

var updatePropertiesMap = [...]func(expr *expr){
	relational: updateRelationProperties,
	scalar:     updateScalarProperties,
	variable:   updateVariableProperties,
}

func (e *expr) updateProperties() {
	updatePropertiesMap[operatorTypeMap[e.op]](e)
}

func (e *expr) resolve(state *queryState, parent *expr) {
	for _, input := range e.inputs() {
		input.resolve(state, e)
	}

	e.resolveBody(state, parent)

	for _, filter := range e.filters() {
		filter.resolve(state, e)
	}

	for i, project := range e.projections() {
		project.resolve(state, e)
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

func (e *expr) resolveBody(state *queryState, parent *expr) {
	switch b := e.body.(type) {
	case nil:

	case *parser.NormalizableTableName:
		tableName, err := b.Normalize()
		if err != nil {
			fatalf("%s", err)
		}
		name := tableName.Table()
		if table, ok := state.catalog[name]; !ok {
			fatalf("unknown table %s", name)
		} else {
			e.body = table
			base := bitmapIndex(len(state.columns))
			state.tables[name] = base
			for i := range table.columnNames {
				state.columns = append(state.columns, columnRef{
					table: table,
					index: columnIndex(i),
				})
				e.inputVars.set(base + bitmapIndex(i))
			}
		}

	case parser.UnqualifiedStar:
		e.inputVars = parent.inputVars

	case parser.UnresolvedName:
		if len(b) != 2 {
			fatalf("unsupported unqualified name: %s", b)
		}
		tableName := string(b[0].(parser.Name))
		colName := string(b[1].(parser.Name))
		if base, ok := state.tables[tableName]; !ok {
			fatalf("unknown table %s", b)
		} else if table, ok := state.catalog[tableName]; !ok {
			fatalf("unknown table %s", b)
		} else if colIndex, ok := table.columns[colName]; !ok {
			fatalf("unknown column %s", b)
		} else {
			e.inputVars.set(base + bitmapIndex(colIndex))
		}

	case *parser.NumVal:

	case parser.NaturalJoinCond:
		e.resolveNaturalJoin(state)

	case *parser.UsingJoinCond:
		e.resolveUsingJoin(state, b.Cols)

	case *parser.ExistsExpr:
		// TODO(peter): unimplemented.

	default:
		unimplemented("%T", e.body)
	}
}

func (e *expr) resolveNaturalJoin(state *queryState) {
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

	e.resolveUsingJoin(state, names)
}

func (e *expr) resolveUsingJoin(state *queryState, names parser.NameList) {
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

func (e *expr) substitute(columns bitmap, replacement *expr) *expr {
	if e.op == variableOp && e.outputVars == columns {
		return replacement
	}

	result := *e
	result.children = append([]*expr(nil), e.children...)

	for i, input := range result.inputs() {
		result.children[i] = input.substitute(columns, replacement)
	}
	result.updateProperties()
	return &result
}

func (e *expr) isFilterCompatible(filter *expr) bool {
	// NB: when pushing down a filter, the filter applies before the projection
	// and thus needs to be compatible with the input variables, not the output
	// variables.
	return (filter.inputVars & e.inputVars) == filter.inputVars
}

func buildEquivalencyMap(filters []*expr) map[bitmap]*expr {
	// Build an equivalency map from any equality predicates.
	var equivalencyMap map[bitmap]*expr
	for _, filter := range filters {
		if filter.op == eqOp {
			left := filter.inputs()[0]
			right := filter.inputs()[1]
			if left.op == variableOp && right.op == variableOp {
				if equivalencyMap == nil {
					equivalencyMap = make(map[bitmap]*expr)
				}
				equivalencyMap[left.outputVars] = right
				equivalencyMap[right.outputVars] = left
			}
		}
	}
	return equivalencyMap
}

// TODO(peter): I'm sure this is incorrect in various cases.
func (e *expr) pushDownFilters(state *queryState) {
	// Push down filters to inputs.
	filters := e.filters()
	// Strip off all of the filters. We'll re-add any filters that couldn't be
	// pushed down.
	e.children = e.children[:e.inputCount+e.projectCount]
	var equivalencyMap map[bitmap]*expr
	var equivalencyMapInited bool

	for _, filter := range filters {
		count := 0
		for _, input := range e.inputs() {
			if input.isFilterCompatible(filter) {
				input.addFilter(filter)
				count++
				continue
			}

			// Check to see if creating a new filter by substitution could be pushed down.
			if !equivalencyMapInited {
				equivalencyMapInited = true
				equivalencyMap = buildEquivalencyMap(filters)
			}
			if substitute, ok := equivalencyMap[filter.inputVars]; ok {
				if input.isFilterCompatible(substitute) {
					newFilter := filter.substitute(filter.inputVars, substitute) //
					input.addFilter(newFilter)
					count++
					continue
				}
			}
		}
		if count == 0 {
			e.addFilter(filter)
		}
	}

	for _, input := range e.inputs() {
		input.updateProperties()
		input.pushDownFilters(state)
	}
	e.updateProperties()
}

func (e *expr) decorrelate(state *queryState) {
	// TODO(peter): In general the simple unnesting phase moves all dependent
	// predicates up the tree as far as possible, potentially beyond joins,
	// selections, group by, etc., until it reaches a point where all its
	// attributes are available from the input. If this happens the dependent
	// join can be transformed into a regular join, as shown by the equivalence
	// explained above. Note that this predicate pull-up happens purely for
	// decorreleation reasons. Further optimization steps might push (parts of)
	// the predicate back down again to filter tuples early on. See "Unnesting
	// Arbitrary Queries".

	// Transform "filter -> exists -> scan" into an inner join. This is not
	// general as it requires there be a single EXISTS filter expression.
	for _, filter := range e.filters() {
		if filter.op == existsOp {
			input := filter.inputs()[0]
			if ifilters := input.filters(); len(ifilters) == 1 {
				ifilter := ifilters[0]
				if ifilter.op == eqOp && ifilter.inputVars != input.inputVars {
					left := *e
					left.children = append([]*expr(nil), e.inputs()...)
					for _, t := range e.filters() {
						if t != filter {
							left.addFilter(t)
						}
					}
					left.projectCount = 0

					join := expr{
						op:         innerJoinOp,
						children:   []*expr{&left, input},
						inputCount: 2,
					}
					join.addFilter(ifilter)
					for _, project := range e.projections() {
						join.addProjection(project)
					}
					*e = join

					input.removeProjections()
					input.removeFilters()
					return
				}
			}
		}
	}
}

type columnIndex uint

type table struct {
	name        string
	columns     map[string]columnIndex
	columnNames []string
}

func (t *table) String() string {
	return fmt.Sprintf("%s (%s)", t.name, strings.Join(t.columnNames, ", "))
}

type columnRef struct {
	// TODO(peter): rather than a table, this should be a relation so that column
	// references can refer to intermediate results in the query.
	table *table
	index columnIndex
}

// queryState holds per-query state such as the tables referenced by the query
// and the mapping from table name to the column index for those tables columns
// within the query.
type queryState struct {
	catalog map[string]*table
	tables  map[string]bitmapIndex
	// query index to table and column index.
	columns []columnRef
}

type executor struct {
	tables map[string]*table
}

func newExecutor() *executor {
	return &executor{
		tables: make(map[string]*table),
	}
}

func (e *executor) exec(sql string) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		panic(err)
	}
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *parser.CreateTable:
			e.createTable(stmt)
		default:
			fmt.Printf("%s\n", stmt)
			expr, state := e.prep(stmt)
			expr.pushDownFilters(state)
			fmt.Printf("%s\n", expr)
		}
	}
}

func (e *executor) prep(stmt parser.Statement) (*expr, *queryState) {
	expr := e.build(stmt)
	// Resolve names and propagate column properties.
	state := &queryState{
		catalog: e.tables,
		tables:  make(map[string]bitmapIndex),
	}
	expr.resolve(state, nil)
	return expr, state
}

func (e *executor) build(stmt parser.Statement) *expr {
	switch stmt := stmt.(type) {
	case *parser.Select:
		return e.buildSelect(stmt)
	case *parser.ParenSelect:
		return e.buildSelect(stmt.Select)
	default:
		unimplemented("%T", stmt)
		return nil
	}
}

func (e *executor) buildTable(table parser.TableExpr) *expr {
	switch source := table.(type) {
	case *parser.NormalizableTableName:
		return &expr{
			op:   scanOp,
			body: source,
		}
	case *parser.AliasedTableExpr:
		return e.buildTable(source.Expr)
	case *parser.ParenTableExpr:
		return e.buildTable(source.Expr)
	case *parser.JoinTableExpr:
		result := &expr{
			op: innerJoinOp,
			children: []*expr{
				e.buildTable(source.Left),
				e.buildTable(source.Right),
			},
			inputCount: 2,
		}

		switch cond := source.Cond.(type) {
		case *parser.OnJoinCond:
			result.addFilter(e.buildExpr(cond.Expr))

		case parser.NaturalJoinCond:
			result.body = cond
		case *parser.UsingJoinCond:
			result.body = cond

		default:
			unimplemented("%T", source.Cond)
		}
		return result

	default:
		unimplemented("%T", table)
		return nil
	}
}

func (e *executor) buildExpr(pexpr parser.Expr) *expr {
	switch t := pexpr.(type) {
	case *parser.ParenExpr:
		return e.buildExpr(t.Expr)

	case *parser.AndExpr:
		return &expr{
			op: andOp,
			children: []*expr{
				e.buildExpr(t.Left),
				e.buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.OrExpr:
		return &expr{
			op: orOp,
			children: []*expr{
				e.buildExpr(t.Left),
				e.buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.NotExpr:
		return &expr{
			op: notOp,
			children: []*expr{
				e.buildExpr(t.Expr),
			},
			inputCount: 1,
		}

	case *parser.BinaryExpr:
		return &expr{
			op: binaryOpMap[t.Operator],
			children: []*expr{
				e.buildExpr(t.Left),
				e.buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.ComparisonExpr:
		return &expr{
			op: comparisonOpMap[t.Operator],
			children: []*expr{
				e.buildExpr(t.Left),
				e.buildExpr(t.Right),
			},
			inputCount: 2,
		}
	case *parser.UnaryExpr:
		return &expr{
			op: unaryOpMap[t.Operator],
			children: []*expr{
				e.buildExpr(t.Expr),
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
				e.buildExpr(t.Subquery),
			},
			inputCount: 1,
		}

	case *parser.Subquery:
		return e.build(t.Select)

	default:
		unimplemented("%T", pexpr)
		return nil
	}
}

func (e *executor) buildSelect(stmt *parser.Select) *expr {
	// TODO: stmt.Limit
	orderBy := stmt.OrderBy

	// TODO: handle other stmt.Select types.
	clause := stmt.Select.(*parser.SelectClause)

	var result *expr
	if clause.From != nil {
		var inputs []*expr
		for _, table := range clause.From.Tables {
			inputs = append(inputs, e.buildTable(table))
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
			result.addFilter(e.buildExpr(clause.Where.Expr))
		}
	}

	if clause.GroupBy != nil {
		result = &expr{
			op:         groupByOp,
			children:   []*expr{result},
			inputCount: 1,
		}
		if clause.Having != nil {
			result.addFilter(e.buildExpr(clause.Having.Expr))
		}
	}

	for _, expr := range clause.Exprs {
		result.addProjection(e.buildExpr(expr.Expr))
	}

	if clause.Distinct {
		result = &expr{
			op:         distinctOp,
			children:   []*expr{result},
			inputCount: 1,
		}
	}

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

func (e *executor) createTable(stmt *parser.CreateTable) {
	tableName, err := stmt.Table.Normalize()
	if err != nil {
		fatalf("%s", err)
	}
	name := tableName.Table()
	if _, ok := e.tables[name]; ok {
		fatalf("table %s already exists", name)
	}
	table := &table{
		name:    name,
		columns: make(map[string]columnIndex),
	}
	e.tables[name] = table

	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *parser.ColumnTableDef:
			if _, ok := table.columns[string(def.Name)]; ok {
				fatalf("column %s already exists", def.Name)
			}
			table.columns[string(def.Name)] = columnIndex(len(table.columnNames))
			table.columnNames = append(table.columnNames, string(def.Name))
		default:
			unimplemented("%T", def)
		}
	}
}
