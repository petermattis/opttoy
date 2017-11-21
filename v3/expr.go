package v3

import (
	"bytes"
	"fmt"
)

// expr is a unified interface for both relational and scalar expressions in a
// query. Expressions have optional inputs. Specific operators also maintain
// additional auxiliary sub-expressions. In particular, projectOp maintains the
// projection expressions, groupByOp maintains the grouping and aggregation
// expressions, and selectOp and joinOps maintain filters. The position of
// these auxiliary expressions within expr.children is specified by an
// exprLayout.
//
// Expressions maintain properties. For relational expressions the properties
// are stored in expr.props. For scalar expressions the properties are stored
// in expr.scalarProps.
//
// Every unique column and every projection (that is more than just a pass
// through of a column) is given a column index within the query. The column
// indexes are global to the query (see queryState.nextVar). For example,
// consider the query:
//
//   SELECT x FROM a WHERE y > 0
//
// There are 2 columns in the above query: x and y. During name resolution, the
// above query becomes:
//
//   SELECT [0] FROM a WHERE [1] > 0
//   -- [0] -> x
//   -- [1] -> y
//
// This is akin to the way parser.IndexedVar works except that we're taking
// care to make the indexes unique across the entire statement.
//
// Relational expressions are composed of inputs and optional auxiliary
// expressions. The output columns are derived by the operator from the inputs
// and stored in expr.props.columns.
//
//   +---------+---------+-------+--------+
//   |  out 0  |  out 1  |  ...  |  out N |
//   +---------+---------+-------+--------+
//   |                operator            |
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
type expr struct {
	op operator
	// Location of the expression in the memo.
	loc memoLoc
	// Child expressions. The interpretation of the children is operator
	// dependent.
	children []*expr
	// Relational properties. Nil for scalar expressions.
	props *relationalProps
	// Scalar properties. Nil for relational expressions.
	scalarProps *scalarProps
	// Physical properties. Nil for scalar expressions and logical relational
	// expressions.
	physicalProps *physicalProps
	// Private data used by this expression. For example, scanOp store a pointer
	// to the underlying table while constOp store a pointer to the constant
	// value.
	private interface{}
}

// exprLayout describe the layout of auxiliary children expressions. The layout
// is operator specific and accessed via the operatorLayout table.
type exprLayout struct {
	numAux       int8 // used internally, no need to specify manually
	aggregations int8
	filters      int8
	groupings    int8
	projections  int8
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

func formatRelational(e *expr, buf *bytes.Buffer, level int) {
	fmt.Fprintf(buf, "%s%v", spaces[:2*level], e.op)
	if !e.props.outputCols.Empty() {
		fmt.Fprintf(buf, " [out=%s]", e.props.outputCols)
	}
	if !e.props.outerCols.Empty() {
		fmt.Fprintf(buf, " [outer=%s]", e.props.outerCols)
	}
	buf.WriteString("\n")
	e.props.format(buf, level+1)
	if e.physicalProps != nil {
		e.physicalProps.format(buf, level+1)
	}
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

func (e *expr) inputs() []*expr {
	return e.children[:len(e.children)-int(e.layout().numAux)]
}

func (e *expr) aux(i int8, op operator) []*expr {
	t := e.children[i : i+1]
	if t[0] == nil {
		return nil
	}
	if t[0].op == op {
		return t[0].children
	}
	return t
}

func (e *expr) addAux1(i int8, op operator, aux *expr) {
	if t := e.children[i]; t == nil {
		e.children[i] = aux
	} else if t.op != op {
		e.children[i] = &expr{
			op:       op,
			children: []*expr{t, aux},
		}
	} else {
		t.children = append(t.children, aux)
	}
}

func (e *expr) addAuxN(i int8, op operator, aux []*expr) {
	if t := e.children[i]; t == nil && len(aux) == 1 {
		e.children[i] = aux[0]
	} else if t == nil {
		e.children[i] = &expr{
			op:       op,
			children: aux,
		}
	} else if t.op != op {
		e.children[i] = &expr{
			op:       op,
			children: make([]*expr, 1+len(aux)),
		}
		e.children[i].children[0] = t
		copy(e.children[i].children[1:], aux)
	} else {
		t.children = append(t.children, aux...)
	}
}

func (e *expr) replaceAuxN(i int8, op operator, aux []*expr) {
	if len(aux) == 1 {
		e.children[i] = aux[0]
	} else if len(aux) > 1 {
		e.children[i] = &expr{
			op:       op,
			children: aux,
		}
	} else {
		e.children[i] = nil
	}
}

func (e *expr) removeAux1(j int8, op operator, aux *expr) {
	if e.children[j] == aux {
		e.children[j] = nil
		return
	}

	exprs := e.aux(j, op)
	for i := range exprs {
		if exprs[i] == aux {
			copy(exprs[i:], exprs[i+1:])
			exprs = exprs[:len(exprs)-1]
			if len(exprs) == 0 {
				e.removeAuxN(j)
			}
			return
		}
	}
	fatalf("expression not found!")
}

func (e *expr) removeAuxN(i int8) {
	e.children[i] = nil
}

func (e *expr) filters() []*expr {
	l := e.layout()
	if l.filters < 0 {
		return nil
	}
	return e.aux(l.filters, listOp)
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

	e.addAux1(e.layout().filters, listOp, f)
}

func (e *expr) addFilters(filters []*expr) {
	for _, f := range filters {
		e.addFilter(f)
	}
}

func (e *expr) removeFilter(f *expr) {
	e.removeAux1(e.layout().filters, listOp, f)
}

func (e *expr) removeFilters() {
	e.removeAuxN(e.layout().filters)
}

func (e *expr) replaceFilters(filters []*expr) {
	e.replaceAuxN(e.layout().filters, listOp, filters)
}

func (e *expr) projections() []*expr {
	return e.aux(e.layout().projections, orderedListOp)
}

func (e *expr) addProjections(exprs []*expr) {
	e.addAuxN(e.layout().projections, orderedListOp, exprs)
}

func (e *expr) groupings() []*expr {
	return e.aux(e.layout().groupings, orderedListOp)
}

func (e *expr) addGroupings(exprs []*expr) {
	e.addAuxN(e.layout().groupings, orderedListOp, exprs)
}

func (e *expr) aggregations() []*expr {
	return e.aux(e.layout().aggregations, orderedListOp)
}

func (e *expr) addAggregation(a *expr) {
	e.addAux1(e.layout().aggregations, orderedListOp, a)
}

func (e *expr) addAggregations(exprs []*expr) {
	e.addAuxN(e.layout().aggregations, orderedListOp, exprs)
}

func (e *expr) setApply() {
	e.op = setApply[e.op]
}

func (e *expr) clearApply() {
	e.op = clearApply[e.op]
}

func (e *expr) hasApply() bool {
	return hasApply[e.op]
}

func (e *expr) isLogical() bool {
	return (e.info().kind() & logicalKind) != 0
}

func (e *expr) isPhysical() bool {
	return (e.info().kind() & physicalKind) != 0
}

func (e *expr) isRelational() bool {
	return (e.info().kind() & relationalKind) != 0
}

func (e *expr) isScalar() bool {
	return (e.info().kind() & scalarKind) != 0
}

func (e *expr) layout() exprLayout {
	return operatorLayout[e.op]
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

func (e *expr) scalarInputCols() bitmap {
	if e.scalarProps == nil {
		return bitmap{}
	}
	return e.scalarProps.inputCols
}

func (e *expr) requiredFilterCols() bitmap {
	var v bitmap
	for _, f := range e.filters() {
		v.UnionWith(f.scalarInputCols())
	}
	return v
}

func (e *expr) requiredInputCols() bitmap {
	exprs := e.children[len(e.inputs()):]
	var v bitmap
	for _, e := range exprs {
		if e == nil {
			continue
		}
		if e.op == listOp || e.op == orderedListOp {
			for _, c := range e.children {
				v.UnionWith(c.scalarInputCols())
			}
		} else {
			v.UnionWith(e.scalarInputCols())
		}
	}
	return v
}

func (e *expr) providedInputCols() bitmap {
	var v bitmap
	for _, input := range e.inputs() {
		v.UnionWith(input.props.outputCols)
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
