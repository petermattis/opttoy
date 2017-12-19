package main

import (
	"fmt"
	"io"
)

func (g *generator) genOps(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	fmt.Fprintf(w, "import (\n")
	fmt.Fprintf(w, "  \"unsafe\"\n")
	fmt.Fprintf(w, ")\n\n")

	fmt.Fprintf(w, "const (\n")
	fmt.Fprintf(w, "  unknownOp operator = iota\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		fmt.Fprintf(w, "  %sOp\n", unTitle(define.Name()))
	}

	fmt.Fprintf(w, ")\n\n")
}
