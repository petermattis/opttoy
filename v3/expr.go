package v3

import (
	"bytes"
	"fmt"
)

// TODO(peter): Rework this documentation. Invariants: only projectOp contains
// projections. Only selectOp and *JoinOp contain filters.

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
	// NB: op, projectCount and filterCount are placed next to each other in
	// order to reduce space wastage due to padding.
	op operator
	// The inputs, projections and filters are all stored in the children slice
	// to minimize overhead. The projectCount and filterCount values delineate
	// the input, projection and filter sub-slices:
	//   inputCount == len(children) - filterCount - aux1Count - aux2Count
	//   inputs:      children[:inputCount]
	//   aux1:        children[inputCount:inputCount + aux1Count]
	//   aux2:        children[inputCount+aux1Count:inputCount + aux1Count + aux2Count]
	//   filters:     children[inputCount + aux1Count + aux2Count + filterCount:]
	filterCount int16
	aux1Count   int16
	aux2Count   int16
	dataIndex   int32
	// The input and output bitmaps specified required inputs and generated
	// outputs. The indexes refer to queryState.columns which is constructed on a
	// per-query basis by the columns required by filters, join conditions, and
	// projections and the new columns generated by projections.
	inputVars  bitmap
	outputVars bitmap
	children   []*expr
	table      *table
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
	if e.inputVars != 0 || e.outputVars != 0 {
		buf.WriteString(" [")
		sep := ""
		if e.inputVars != 0 {
			fmt.Fprintf(buf, "in=%s", e.inputVars)
			sep = " "
		}
		if e.outputVars != 0 {
			sep = " "
			fmt.Fprintf(buf, "%sout=%s", sep, e.outputVars)
		}
		buf.WriteString("]")
	}
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

func (e *expr) inputCount() int {
	return len(e.children) - int(e.filterCount+e.aux1Count+e.aux2Count)
}

func (e *expr) inputs() []*expr {
	return e.children[:e.inputCount()]
}

func (e *expr) filters() []*expr {
	filterStart := len(e.children) - int(e.filterCount)
	return e.children[filterStart:]
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
	e.filterCount++
}

func (e *expr) removeFilters() {
	filterStart := len(e.children) - int(e.filterCount)
	e.children = e.children[:filterStart]
	e.filterCount = 0
}

func (e *expr) aux1() []*expr {
	aux1Start := e.inputCount()
	return e.children[aux1Start : aux1Start+int(e.aux1Count)]
}

func (e *expr) addAux1(exprs []*expr) {
	aux2Start := len(e.children) - int(e.filterCount+e.aux2Count)
	e.children = append(e.children, exprs...)
	copy(e.children[aux2Start+len(exprs):], e.children[aux2Start:])
	copy(e.children[aux2Start:], exprs)
	e.aux1Count += int16(len(exprs))
}

func (e *expr) aux2() []*expr {
	aux2Start := e.inputCount() + int(e.aux1Count)
	return e.children[aux2Start : aux2Start+int(e.aux2Count)]
}

func (e *expr) addAux2(exprs []*expr) {
	filterStart := len(e.children) - int(e.filterCount)
	e.children = append(e.children, exprs...)
	copy(e.children[filterStart+len(exprs):], e.children[filterStart:])
	copy(e.children[filterStart:], exprs)
	e.aux2Count += int16(len(exprs))
}

func (e *expr) info() *operatorInfo {
	return &operatorTab[e.op]
}

func (e *expr) updateProperties() {
	e.info().updateProperties(e)
}
