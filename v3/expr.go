package v3

import (
	"bytes"
	"fmt"
)

// expr is a unified interface for both relational and scalar expressions in a
// query. Expressions have optional inputs and filters. Specific operators also
// maintain additional expressions in the aux1 and aux2 slots. In particular,
// projectOp stores the projection expressions in aux1, groupByOp stores the
// grouping expressions in aux1 and the aggregations in aux2 and orderByOp
// stores the sorting expressions in aux2.
//
// Expressions contain a pointer to their logical properties. For scalar
// expressions, the logical properties points to the context in which the
// scalar is defined.
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
	// NB: op and auxBits are placed next to each other in order to reduce space
	// wastage due to padding.
	op operator
	// The inputs, projections and filters are all stored in the children slice
	// to minimize overhead. auxBits indicates which of these auxiliary
	// expressions is present.
	auxBits uint16
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
	props     *logicalProps
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
			e.format(buf, level+2)
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
	return len(e.children) - (e.filterPresent() + e.aux1Present() + e.aux2Present())
}

func (e *expr) inputs() []*expr {
	return e.children[:e.inputCount()]
}

const (
	auxFilterBit = iota
	aux1Bit
	aux2Bit
	auxApplyBit
)

func (e *expr) filterPresent() int {
	return int((e.auxBits >> auxFilterBit) & 1)
}

func (e *expr) filters() []*expr {
	if e.filterPresent() == 0 {
		return nil
	}
	i := len(e.children) - 1
	f := e.children[i:]
	if f[0].op == andOp {
		return f[0].children
	}
	return f
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

	if e.filterPresent() == 0 {
		e.auxBits |= 1 << auxFilterBit
		e.children = append(e.children, f)
	} else {
		i := len(e.children) - 1
		if t := e.children[i]; t.op != andOp {
			e.children[i] = &expr{
				op:       andOp,
				children: []*expr{t, f},
				props:    t.props,
			}
		} else {
			t.children = append(t.children, f)
		}
	}
}

func (e *expr) addFilters(filters []*expr) {
	for _, f := range filters {
		e.addFilter(f)
	}
}

func (e *expr) removeFilter(f *expr) {
	filters := e.filters()
	for i := range filters {
		if filters[i] == f {
			copy(filters[i:], filters[i+1:])
			filters = filters[:len(filters)-1]
			if len(filters) == 0 {
				e.removeFilters()
			}
			return
		}
	}
	fatalf("filter not found!")
}

func (e *expr) removeFilters() {
	filterStart := len(e.children) - e.filterPresent()
	e.children = e.children[:filterStart]
	e.auxBits &^= 1 << auxFilterBit
}

func (e *expr) aux1Present() int {
	return int((e.auxBits >> aux1Bit) & 1)
}

func (e *expr) aux1Index() int {
	if e.aux1Present() == 0 {
		return -1
	}
	return len(e.children) - 1 - e.filterPresent()
}

func (e *expr) aux1() []*expr {
	i := e.aux1Index()
	if i < 0 {
		return nil
	}
	return e.children[i].children
}

func (e *expr) addAux1(exprs []*expr) {
	if e.aux1Present() == 0 {
		e.auxBits |= 1 << aux1Bit
		e.children = append(e.children, nil)
		i := e.aux1Index()
		copy(e.children[i+1:], e.children[i:])
		e.children[i] = &expr{
			op:       andOp,
			children: exprs,
			props:    e.props,
		}
	} else {
		i := e.aux1Index()
		aux1 := e.children[i]
		aux1.children = append(aux1.children, exprs...)
	}
}

func (e *expr) aux2Present() int {
	return int((e.auxBits >> aux2Bit) & 1)
}

func (e *expr) aux2Index() int {
	if e.aux2Present() == 0 {
		return -1
	}
	return len(e.children) - 1 - int(e.filterPresent()+e.aux1Present())
}

func (e *expr) aux2() []*expr {
	i := e.aux2Index()
	if i < 0 {
		return nil
	}
	return e.children[i].children
}

func (e *expr) addAux2(exprs []*expr) {
	if e.aux2Present() == 0 {
		e.auxBits |= 1 << aux2Bit
		e.children = append(e.children, nil)
		i := e.aux2Index()
		copy(e.children[i+1:], e.children[i:])
		e.children[i] = &expr{
			op:       andOp,
			children: exprs,
			props:    e.props,
		}
	} else {
		i := e.aux2Index()
		aux2 := e.children[i]
		aux2.children = append(aux2.children, exprs...)
	}
}

func (e *expr) projections() []*expr {
	if e.op != projectOp {
		fatalf("%s: invalid use of projections", e.op)
	}
	return e.aux1()
}

func (e *expr) addProjections(exprs []*expr) {
	if e.op != projectOp {
		fatalf("%s: invalid use of projections", e.op)
	}
	e.addAux1(exprs)
}

func (e *expr) groupings() []*expr {
	if e.op != groupByOp {
		fatalf("%s: invalid use of groupings", e.op)
	}
	return e.aux1()
}

func (e *expr) addGroupings(exprs []*expr) {
	if e.op != groupByOp {
		fatalf("%s: invalid use of groupings", e.op)
	}
	e.addAux1(exprs)
}

func (e *expr) aggregations() []*expr {
	if e.op != groupByOp {
		fatalf("%s: invalid use of aggregations", e.op)
	}
	return e.aux2()
}

func (e *expr) addAggregations(exprs []*expr) {
	if e.op != groupByOp {
		fatalf("%s: invalid use of aggregations", e.op)
	}
	e.addAux2(exprs)
}

func (e *expr) setApply() {
	e.auxBits |= 1 << auxApplyBit
}

func (e *expr) clearApply() {
	e.auxBits &^= 1 << auxApplyBit
}

func (e *expr) hasApply() bool {
	return (e.auxBits & (1 << auxApplyBit)) != 0
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

func (e *expr) updateProps() {
	e.info().updateProps(e)
}
