package v3

import (
	"bytes"
	"fmt"
)

// Offsets of operator specific sub-expressions from the end of the
// expr.children slice.
const (
	filterOffset      = 0
	projectionOffset  = 1
	aggregationOffset = 1
	groupingOffset    = 2
)

// expr is a unified interface for both relational and scalar expressions in a
// query. Expressions have optional inputs and filters. Specific operators also
// maintain additional sub-expressions. In particular, projectOp stores the
// projection expressions at projectionOffset from the end of the children
// slice, groupByOp stores the grouping expressions at groupingOffset and the
// aggregations aggregationOffset. All relational expressions store filters at
// filterOffset.
//
// Expressions contain a pointer to their relational properties. For scalar
// expressions, the relational properties are nil.
//
// Every unique column and every projection (that is more than just a pass
// through of a variable) is given a variable index within the query. The
// variable indexes are global to the query (see queryState.nextVar). For
// example, consider the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 variables in the above query: x and y. During name resolution,
// the above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
//
// This is akin to the way parser.IndexedVar works except that we're taking
// care to make the indexes unique across the entire statement.
//
// Expressions keep track of required input variables (a.k.a. free variables)
// in the expr.inputVars bitmap. The required input variables for a scalar
// expression are any variables used by the expression. The required input
// variables for a relational expression are on variables required by filters
// or projections that are not otherwise provided by inputs.
//
// For relational expressions, a non-empty input variables set indicates a
// correlated subquery.
//
// For scalar expressions the input variables bitmap allows an easy
// determination of whether the expression is constant (the bitmap is empty)
// and, if not, which variables it uses. Predicate push down can use this
// bitmap to quickly determine whether a filter can be pushed below a
// relational operator.
//
// Relational expressions are composed of inputs, optional filters and optional
// auxiliary expressions. The output columns are derived by the operator from
// the inputs and stored in expr.props.columns.
//
//   +---------+---------+-------+--------+
//   |  out 0  |  out 1  |  ...  |  out N |
//   +---------+---------+-------+--------+
//   |             filters                |
//   +------------------------------------+
//   |        operator (aux1, aux2)       |
//   +---------+---------+-------+--------+
//   |  in 0   |  in 1   |  ...  |  in N  |
//   +---------+---------+-------+--------+
//
// Note that the filters for a relational expression do not affect the result
// columns for the expression. The filters for a relational expression can be
// seen as a "select" operator that exists after the outputs of any relational
// expression. Note that unlike the traditional relational algebra and extended
// relational algebra, there is no explicit "select" operator.
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
type expr struct {
	// NB: op, extran and apply are placed next to each other in order to reduce
	// space wastage due to padding.
	op operator
	// The inputs, projections and filters are all stored in the children slice
	// to minimize overhead. extra indicates how many of these extra expressions
	// are present.
	extra uint8
	apply bool
	// Location of the expression in the memo.
	loc memoLoc
	// The input vars bitmap specifies required input variables (a.k.a. free
	// variables) that are not otherwise provided by inputs. For scalar
	// operators, this will by the set of all variables referenced by the scalar
	// expression. For relational operators, this is the set of variables needed
	// by the operator that are not provided by the inputs. The only reason a
	// relational operator will have input variables that are not provided by its
	// inputs are correlated subqueries. The indexes are allocated via
	// queryState.nextVar.
	inputVars bitmap
	children  []*expr
	// Relational properties for relational expressions. This field is nil for
	// scalar expressions.
	props *relationalProps
	// Private data used by this expression. For example, scanOp store a pointer
	// to the underlying table while constOp store a pointer to the constant
	// value.
	private interface{}
}

func (e *expr) String() string {
	var buf bytes.Buffer
	e.format(&buf, 0)
	return buf.String()
}

func (e *expr) MemoString() string {
	var format func(e *expr, buf *bytes.Buffer, level int)
	format = func(e *expr, buf *bytes.Buffer, level int) {
		fmt.Fprintf(buf, "%s[%s] %s\n", spaces[:2*level], e.loc, e.op)
		for _, c := range e.children {
			if c != nil {
				format(c, buf, level+1)
			}
		}
	}

	var buf bytes.Buffer
	format(e, &buf, 0)
	return buf.String()
}

func (e *expr) format(buf *bytes.Buffer, level int) {
	e.info().format(e, buf, level)
}

func (e *expr) formatVars(buf *bytes.Buffer) {
	if e.inputVars != 0 {
		fmt.Fprintf(buf, " [in=%s]", e.inputVars)
	}
}

func formatRelational(e *expr, buf *bytes.Buffer, level int) {
	fmt.Fprintf(buf, "%s%v", spaces[:2*level], e.op)
	if e.hasApply() {
		buf.WriteString(" (apply)")
	}
	e.formatVars(buf)
	buf.WriteString("\n")
	e.props.format(buf, level+1)
}

func formatExprs(buf *bytes.Buffer, title string, exprs []*expr, level int) {
	if len(exprs) > 0 {
		indent := spaces[:2*level]
		fmt.Fprintf(buf, "%s  %s:\n", indent, title)
		for _, e := range exprs {
			if e != nil {
				e.format(buf, level+2)
			}
		}
	}
}

func (e *expr) clone() *expr {
	t := *e
	t.children = make([]*expr, len(e.children))
	copy(t.children, e.children)
	return &t
}

func (e *expr) inputCount() int {
	return len(e.children) - int(e.extra)
}

func (e *expr) inputs() []*expr {
	return e.children[:e.inputCount()]
}

func (e *expr) aux(offset int) []*expr {
	if int(e.extra) <= offset {
		fatalf("%s: invalid use of auxiliary expression", e.op)
	}
	i := len(e.children) - 1 - offset
	t := e.children[i : i+1]
	if t[0] == nil {
		return nil
	}
	if t[0].op == listOp {
		return t[0].children
	}
	return t
}

func (e *expr) addAux1(offset int, aux *expr) {
	if int(e.extra) <= offset {
		fatalf("%s: invalid use of auxiliary expression", e.op)
	}

	i := len(e.children) - 1 - offset
	if t := e.children[i]; t == nil {
		e.children[i] = aux
	} else if t.op != listOp {
		e.children[i] = &expr{
			op:       listOp,
			children: []*expr{t, aux},
		}
	} else {
		t.children = append(t.children, aux)
	}
}

func (e *expr) addAuxN(offset int, aux []*expr) {
	if int(e.extra) <= offset {
		fatalf("%s: invalid use of auxiliary expression", e.op)
	}

	i := len(e.children) - 1 - offset
	if t := e.children[i]; t == nil && len(aux) == 1 {
		e.children[i] = aux[0]
	} else if t == nil {
		e.children[i] = &expr{
			op:       listOp,
			children: aux,
		}
	} else if t.op != listOp {
		e.children[i] = &expr{
			op:       listOp,
			children: make([]*expr, 1+len(aux)),
		}
		e.children[i].children[0] = t
		copy(e.children[i].children[1:], aux)
	} else {
		t.children = append(t.children, aux...)
	}
}

func (e *expr) replaceAuxN(offset int, aux []*expr) {
	if int(e.extra) <= offset {
		fatalf("%s: invalid use of auxiliary expression", e.op)
	}

	i := len(e.children) - 1 - offset
	if len(aux) == 1 {
		e.children[i] = aux[0]
	} else {
		e.children[i] = &expr{
			op:       listOp,
			children: aux,
		}
	}
}

func (e *expr) removeAux1(offset int, aux *expr) {
	j := len(e.children) - 1 - offset
	if e.children[j] == aux {
		e.children[j] = nil
		return
	}

	exprs := e.aux(offset)
	for i := range exprs {
		if exprs[i] == aux {
			copy(exprs[i:], exprs[i+1:])
			exprs = exprs[:len(exprs)-1]
			if len(exprs) == 0 {
				e.removeAuxN(offset)
			}
			return
		}
	}
	fatalf("expression not found!")
}

func (e *expr) removeAuxN(offset int) {
	i := len(e.children) - 1 - offset
	e.children[i] = nil
}

func (e *expr) filters() []*expr {
	return e.aux(filterOffset)
}

func (e *expr) addFilter(f *expr) {
	// Recursively flatten AND expressions when adding them as a filter. The
	// filters for an expression are implicitly AND'ed together (i.e. they are in
	// conjunctive normal form).
	if f.op == andOp || f.op == listOp {
		for _, input := range f.inputs() {
			e.addFilter(input)
		}
		return
	}

	e.addAux1(filterOffset, f)
}

func (e *expr) addFilters(filters []*expr) {
	for _, f := range filters {
		e.addFilter(f)
	}
}

func (e *expr) removeFilter(f *expr) {
	e.removeAux1(filterOffset, f)
}

func (e *expr) removeFilters() {
	e.removeAuxN(filterOffset)
}

func (e *expr) replaceFilters(filters []*expr) {
	e.replaceAuxN(filterOffset, filters)
}

func (e *expr) projections() []*expr {
	if e.op != projectOp {
		fatalf("%s: invalid use of projections", e.op)
	}
	return e.aux(projectionOffset)
}

func (e *expr) addProjections(exprs []*expr) {
	if e.op != projectOp {
		fatalf("%s: invalid use of projections", e.op)
	}
	e.addAuxN(projectionOffset, exprs)
}

func (e *expr) groupings() []*expr {
	if e.op != groupByOp {
		fatalf("%s: invalid use of groupings", e.op)
	}
	return e.aux(groupingOffset)
}

func (e *expr) addGroupings(exprs []*expr) {
	if e.op != groupByOp {
		fatalf("%s: invalid use of groupings", e.op)
	}
	e.addAuxN(groupingOffset, exprs)
}

func (e *expr) aggregations() []*expr {
	if e.op != groupByOp {
		fatalf("%s: invalid use of aggregations", e.op)
	}
	return e.aux(aggregationOffset)
}

func (e *expr) addAggregation(a *expr) {
	if e.op != groupByOp {
		fatalf("%s: invalid use of aggregations", e.op)
	}
	e.addAux1(aggregationOffset, a)
}

func (e *expr) addAggregations(exprs []*expr) {
	if e.op != groupByOp {
		fatalf("%s: invalid use of aggregations", e.op)
	}
	e.addAuxN(aggregationOffset, exprs)
}

func (e *expr) setApply() {
	e.apply = true
}

func (e *expr) clearApply() {
	e.apply = false
}

func (e *expr) hasApply() bool {
	return e.apply
}

func (e *expr) isRelational() bool {
	return e.info().kind() == relationalKind
}

func (e *expr) isScalar() bool {
	return e.info().kind() == scalarKind
}

func (e *expr) info() operatorInfo {
	return operatorTab[e.op]
}

func (e *expr) initKeys(state *queryState) {
	e.info().initKeys(e, state)
}

func (e *expr) initProps() {
	if e.props != nil {
		e.props.init()
	}
	e.info().updateProps(e)
}

func (e *expr) updateProps() {
	e.info().updateProps(e)
}

func (e *expr) requiredInputVars() bitmap {
	return e.info().requiredInputVars(e)
}

func (e *expr) providedInputVars() bitmap {
	var v bitmap
	for _, input := range e.inputs() {
		v |= input.props.outputVars
	}
	return v
}

func (e *expr) equal(b *expr) bool {
	if e.op != b.op {
		return false
	}

	if len(e.children) != len(b.children) {
		return false
	}
	for i := range e.children {
		if !e.children[i].equal(b.children[i]) {
			return false
		}
	}
	return e.private == b.private
}
