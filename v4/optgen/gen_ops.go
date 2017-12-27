package main

import (
	"bytes"
	"fmt"
	"io"
)

func (g *generator) genOps(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	fmt.Fprintf(w, "const (\n")
	fmt.Fprintf(w, "  UnknownOp Operator = iota\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		fmt.Fprintf(w, "  %sOp\n", define.Name())
	}

	fmt.Fprintf(w, ")\n\n")

	// Generate op names and indexes.
	var names bytes.Buffer
	var indexes bytes.Buffer

	fmt.Fprint(&names, "unknown")
	fmt.Fprint(&indexes, "0, ")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)

		fmt.Fprintf(&indexes, "%d, ", names.Len())

		// Trim the Op suffix and make all lowercase.
		fmt.Fprint(&names, unTitle(define.Name()))
	}

	fmt.Fprintf(w, "const opNames = \"%s\"\n\n", names.String())

	fmt.Fprintf(w, "var opIndexes = [...]uint32{%s%d}\n\n", indexes.String(), names.Len())
}
