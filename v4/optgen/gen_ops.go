package main

import (
	"bytes"
	"fmt"
	"io"
	"unicode"
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

		// Trim the Op suffix and convert to "dash case".
		fmt.Fprint(&names, dashCase(define.Name()))
	}

	fmt.Fprintf(w, "const opNames = \"%s\"\n\n", names.String())

	fmt.Fprintf(w, "var opIndexes = [...]uint32{%s%d}\n\n", indexes.String(), names.Len())
}

// dashCase converts camel-case identifiers into "dash case", where uppercase
// letters in the middle of the identifier are replaced by a dash followed
// by the lowercase version the letter. Example:
//   InnerJoinApply => inner-join-apply
func dashCase(s string) string {
	var buf bytes.Buffer

	for i, ch := range s {
		if unicode.IsUpper(ch) {
			if i != 0 {
				buf.WriteByte('-')
			}

			buf.WriteRune(unicode.ToLower(ch))
		} else {
			buf.WriteRune(ch)
		}
	}

	return buf.String()
}
