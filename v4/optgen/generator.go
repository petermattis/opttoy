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
	pkg     string
	root    *RootExpr
	opIndex map[string]*DefineExpr
	unique  map[string]bool
}

func NewGenerator(pkg string, root *RootExpr) *generator {
	return &generator{pkg: pkg, root: root, opIndex: createOpIndex(root)}
}

func (g *generator) lookupOp(opName string) *DefineExpr {
	return g.opIndex[opName]
}

func (g *generator) lookupField(opName string, fieldPos int) *DefineFieldExpr {
	define, ok := g.opIndex[opName]
	if !ok {
		panic(fmt.Sprintf("unrecognized op '%s'", opName))
	}

	return define.Fields()[fieldPos].AsDefineField()
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

func (g *generator) uniquify(name string) string {
	try := name
	for i := 2; ; i++ {
		_, ok := g.unique[try]
		if !ok {
			break
		}

		try = fmt.Sprintf("%s%d", name, i)
	}

	return try
}

func createOpIndex(root *RootExpr) map[string]*DefineExpr {
	opIndex := make(map[string]*DefineExpr)

	for _, elem := range root.Defines().All() {
		define := elem.AsDefine()
		opIndex[define.Name()] = define
	}

	return opIndex
}

func unTitle(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToLower(rune), name[size:])
}

func mapType(typ string) string {
	switch typ {
	case "Expr":
		return "exprOffset"

	case "ExprList":
		return "listID"

	default:
		return "privateID"
	}
}
