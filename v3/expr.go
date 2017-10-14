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
		if columns := e.columns(); e.inputVars != 0 || e.outputVars != 0 || len(columns) > 0 {
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
			if len(columns) > 0 {
				fmt.Fprintf(buf, "%scols=", sep)
				for i := range columns {
					if i > 0 {
						buf.WriteString(",")
					}
					fmt.Fprintf(buf, "%d", columns[i])
				}
			}
			buf.WriteString("]")
		}

		buf.WriteString("\n")
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

func (e *expr) replaceProjection(p *expr, r []*expr) {
	for i, project := range e.projections() {
		if project == p {
			e.children = append(e.children, r[1:]...)
			pos := int(e.inputCount) + i
			copy(e.children[pos+len(r):], e.children[pos+1:])
			copy(e.children[pos:], r)
			e.projectCount += int16(len(r) - 1)
			return
		}
	}
	fatalf("not reached")
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

func (e *expr) info() *operatorInfo {
	return &operatorTab[e.op]
}

// columns returns the column indexes in the order they are output by the
// expression. The column indexes are computed from the output vars of the
// projections.
func (e *expr) columns() []bitmapIndex {
	return e.info().columns(e)
}

func (e *expr) updateProperties() {
	e.info().updateProperties(e)
}
