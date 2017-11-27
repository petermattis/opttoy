package main

import (
	"fmt"
	"io"
)

func (g *generator) generateOps(w io.Writer) {
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

	fmt.Fprintf(w, "type opConvertFunc func(m *memoExpr) expr\n")
	fmt.Fprintf(w, "var opConvertLookup = []opConvertFunc{\n")
	fmt.Fprintf(w, "  nil,\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		name := unTitle(define.Name())
		fmt.Fprintf(w, "  func(m *memoExpr) expr { return (*%sExpr)(unsafe.Pointer(m)) },\n", name)
	}

	fmt.Fprintf(w, "}\n\n")

	fmt.Fprintf(w, "func (m *memoExpr) asExpr() expr {\n")
	fmt.Fprintf(w, "  return opConvertLookup[m.op](m)\n")
	fmt.Fprintf(w, "}\n\n")
}
