package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

const useGoFmt = true

type generator struct {
	pkg      string
	compiled CompiledExpr
	unique   map[string]bool
}

func NewGenerator(pkg string, compiled CompiledExpr) *generator {
	return &generator{pkg: pkg, compiled: compiled}
}

func (g *generator) GenerateExprs(w io.Writer) error {
	return g.generate(w, g.genExprs)
}

func (g *generator) GenerateFactory(w io.Writer) error {
	return g.generate(w, g.genFactory)
}

func (g *generator) GenerateOps(w io.Writer) error {
	return g.generate(w, g.genOps)
}

func (g *generator) GenerateOptimizer(w io.Writer) error {
	return g.generate(w, g.genOptimizer)
}

func (g *generator) generate(w io.Writer, genFunc func(w io.Writer)) error {
	var buf bytes.Buffer
	genFunc(&buf)

	var b []byte
	var err error

	if useGoFmt {
		b, err = format.Source(buf.Bytes())
		if err != nil {
			// Write out incorrect source for easier debugging.
			b = buf.Bytes()
		}
	} else {
		b = buf.Bytes()
	}

	w.Write(b)
	return err
}

func (g *generator) lookupFieldName(matchFields *MatchFieldsExpr, index int) string {
	define := g.compiled.LookupDefinition(matchFields.OpName())
	defineField := define.Fields()[index].(*DefineFieldExpr)
	return unTitle(defineField.Name())
}

func (g *generator) makeUnique(s string) string {
	try := s
	for i := 2; ; i++ {
		_, ok := g.unique[try]
		if !ok {
			g.unique[try] = true
			return try
		}

		try = fmt.Sprintf("%s%d", s, i)
	}
}

type matchWriter struct {
	writer  io.Writer
	nesting int
}

func (w *matchWriter) nest(format string, args ...interface{}) {
	w.writeIndent(format, args...)
	w.nesting++
}

func (w *matchWriter) write(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) writeIndent(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, strings.Repeat("  ", w.nesting))
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) unnest(n int) {
	for ; n > 0; n-- {
		w.nesting--
		fmt.Fprintf(w.writer, strings.Repeat("  ", w.nesting))
		fmt.Fprintf(w.writer, "}\n")
	}
}

func unTitle(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToLower(rune), name[size:])
}

func mapType(typ string) string {
	switch typ {
	case "Expr":
		return "groupID"

	case "ExprList":
		return "listID"

	default:
		return "privateID"
	}
}
