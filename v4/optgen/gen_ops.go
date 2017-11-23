package main

import (
	"fmt"
	"io"
)

func (g *generator) generateOps(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	fmt.Fprintf(w, "const (\n")
	fmt.Fprintf(w, "  unknownOp operator = iota\n\n")

	for _, elem := range g.root.Defines().All() {
		define := elem.AsDefine()
		fmt.Fprintf(w, "  %sOp\n", unTitle(define.Name()))
	}

	fmt.Fprintf(w, ")\n\n")

	fmt.Fprintf(w, "type opConvertFunc func(m *memoExpr) baseExpr\n")
	fmt.Fprintf(w, "var opConvertLookup = []opConvertFunc{\n")
	fmt.Fprintf(w, "  nil,\n\n")

	for _, elem := range g.root.Defines().All() {
		define := elem.AsDefine()
		name := unTitle(define.Name())
		fmt.Fprintf(w, "  func(m *memoExpr) baseExpr { return (*%sExpr)(unsafe.Pointer(m)) },\n", name)
	}

	fmt.Fprintf(w, "}\n\n")

	fmt.Fprintf(w, "func (m *memoExpr) asBaseExpr() baseExpr {\n")
	fmt.Fprintf(w, "  return opConvertLookup[m.op](m)\n")
	fmt.Fprintf(w, "}\n\n")
}
