package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
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
	return g.generate(w, g.generateExprs)
}

func (g *generator) GenerateFactory(w io.Writer) error {
	return g.generate(w, g.generateFactory)
}

func (g *generator) GenerateOps(w io.Writer) error {
	return g.generate(w, g.generateOps)
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
