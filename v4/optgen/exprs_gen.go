package main

import (
	"fmt"
	"io"
)

type exprsGen struct {
	compiled CompiledExpr
	w        io.Writer
}

func (g *exprsGen) generate(compiled CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w

	fmt.Fprintf(g.w, "import (\n")
	fmt.Fprintf(g.w, "  \"crypto/md5\"\n")
	fmt.Fprintf(g.w, "  \"unsafe\"\n")
	fmt.Fprintf(g.w, ")\n\n")

	g.genChildCountLookup()
	g.genChildGroupLookup()
	g.genPrivateFieldLookup()
	g.genTagLookup()
	g.genIsTag()

	for _, define := range g.compiled.Defines() {
		// Skip enforcers, since they are not memoized.
		if define.HasTag("Enforcer") {
			continue
		}

		g.genExprFuncs(define)
		g.genMemoFuncs(define)
	}
}

func (g *exprsGen) genChildCountLookup() {
	// Generate child count lookup table.
	fmt.Fprintf(g.w, "type childCountLookupFunc func(e *Expr) int\n")

	fmt.Fprintf(g.w, "var childCountLookup = []childCountLookupFunc{\n")
	fmt.Fprintf(g.w, "  // UnknownOp\n")
	fmt.Fprintf(g.w, "  func(e *Expr) int {\n")
	fmt.Fprintf(g.w, "    panic(\"op type not initialized\")\n")
	fmt.Fprintf(g.w, "  },\n\n")

	for _, define := range g.compiled.Defines() {
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		varName := exprType

		fmt.Fprintf(g.w, "  // %sOp\n", define.Name())
		fmt.Fprintf(g.w, "  func(e *Expr) int {\n")

		count := len(define.Fields())
		if define.PrivateField() != nil {
			count--
		}

		list := define.ListField()
		if list != nil {
			fmt.Fprintf(g.w, "    %s := (*%s)(unsafe.Pointer(e.mem.lookupExpr(e.offset)))\n", varName, exprType)
			fmt.Fprintf(g.w, "    return %d + int(%s.%s.len)\n", count-1, varName, unTitle(list.Name()))
		} else {
			fmt.Fprintf(g.w, "    return %d\n", count)
		}

		fmt.Fprintf(g.w, "  },\n\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) genChildGroupLookup() {
	// Generate child group lookup table.
	fmt.Fprintf(g.w, "type childGroupLookupFunc func(e *Expr, n int) GroupID\n")

	fmt.Fprintf(g.w, "var childGroupLookup = []childGroupLookupFunc{\n")
	fmt.Fprintf(g.w, "  // UnknownOp\n")
	fmt.Fprintf(g.w, "  func(e *Expr, n int) GroupID {\n")
	fmt.Fprintf(g.w, "    panic(\"op type not initialized\")\n")
	fmt.Fprintf(g.w, "  },\n\n")

	for _, define := range g.compiled.Defines() {
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		varName := exprType

		fmt.Fprintf(g.w, "  // %sOp\n", define.Name())
		fmt.Fprintf(g.w, "  func(e *Expr, n int) GroupID {\n")

		count := len(define.Fields())
		if define.PrivateField() != nil {
			count--
		}

		if count == 0 {
			fmt.Fprintf(g.w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(g.w, "  },\n\n")
			continue
		}

		if define.HasTag("Enforcer") {
			// Enforcers have a single child which is the same group they're in.
			fmt.Fprintf(g.w, "    if n == 0 {\n")
			fmt.Fprintf(g.w, "      return e.group\n")
			fmt.Fprintf(g.w, "    }\n\n")

			fmt.Fprintf(g.w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(g.w, "  },\n\n")
			continue
		}

		fmt.Fprintf(g.w, "    %s := (*%s)(unsafe.Pointer(e.mem.lookupExpr(e.offset)))\n\n", varName, exprType)

		fmt.Fprintf(g.w, "  switch n {\n")

		for index, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsPrivateType() {
				// Don't include private field.
				break
			}

			if field.IsListType() {
				fmt.Fprintf(g.w, "  default:\n")
				fmt.Fprintf(g.w, "    list := e.mem.lookupList(%s.%s)\n", varName, unTitle(field.Name()))
				fmt.Fprintf(g.w, "    return list[n - %d]", index)
				fmt.Fprintf(g.w, "  }\n")
				break
			}

			fmt.Fprintf(g.w, "  case %d:\n", index)
			fmt.Fprintf(g.w, "    return %s.%s\n", varName, unTitle(field.Name()))
		}

		if define.ListField() == nil {
			fmt.Fprintf(g.w, "  default:\n")
			fmt.Fprintf(g.w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(g.w, "  }\n")
		}

		fmt.Fprintf(g.w, "  },\n\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) genPrivateFieldLookup() {
	// Generate private field lookup table.
	fmt.Fprintf(g.w, "type privateLookupFunc func(e *Expr) PrivateID\n")

	fmt.Fprintf(g.w, "var privateLookup = []privateLookupFunc{\n")
	fmt.Fprintf(g.w, "  // UnknownOp\n")
	fmt.Fprintf(g.w, "  func(e *Expr) PrivateID {\n")
	fmt.Fprintf(g.w, "    panic(\"op type not initialized\")\n")
	fmt.Fprintf(g.w, "  },\n\n")

	for _, define := range g.compiled.Defines() {
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		varName := unTitle(exprType)

		fmt.Fprintf(g.w, "  // %sOp\n", define.Name())
		fmt.Fprintf(g.w, "  func(e *Expr) PrivateID {\n")

		private := define.PrivateField()
		if private != nil {
			fmt.Fprintf(g.w, "    %s := (*%s)(unsafe.Pointer(e.mem.lookupExpr(e.offset)))\n", varName, exprType)
			fmt.Fprintf(g.w, "    return %s.%s\n", varName, unTitle(private.Name()))
		} else {
			fmt.Fprintf(g.w, "    return 0\n")
		}

		fmt.Fprintf(g.w, "  },\n\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) genTagLookup() {
	// Generate lookup tables that indicate whether an expression is associated
	// with a particular tag.
	for _, tag := range g.compiled.DefineTags() {
		if tag == "Custom" {
			// Don't create method, since this is compiler directive.
			continue
		}

		fmt.Fprintf(g.w, "var is%sLookup = []bool{\n", tag)
		fmt.Fprintf(g.w, "  false, // UnknownOp\n\n")

		for _, define := range g.compiled.Defines() {
			fmt.Fprintf(g.w, "  %v, // %sOp\n", define.HasTag(tag), define.Name())
		}

		fmt.Fprintf(g.w, "}\n\n")
	}
}

func (g *exprsGen) genIsTag() {
	// Add isXXX() tag methods on expr for each definition tag.
	for _, tag := range g.compiled.DefineTags() {
		if tag == "Custom" {
			// Don't create method, since this is compiler directive.
			continue
		}

		fmt.Fprintf(g.w, "func (e *Expr) Is%s() bool {\n", tag)
		fmt.Fprintf(g.w, "  return is%sLookup[e.op]\n", tag)
		fmt.Fprintf(g.w, "}\n\n")
	}
}

func (g *exprsGen) genExprFuncs(define *DefineExpr) {
	exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

	// Generate the expression struct.
	fmt.Fprintf(g.w, "type %s struct {\n", exprType)
	fmt.Fprintf(g.w, "  memoExpr\n")

	for _, elem2 := range define.Fields() {
		field := elem2.(*DefineFieldExpr)
		fmt.Fprintf(g.w, "  %s %s\n", unTitle(field.Name()), mapType(field.Type()))
	}

	fmt.Fprintf(g.w, "}\n\n")

	// Generate the Fingerprint method.
	fmt.Fprintf(g.w, "func (e *%s) fingerprint() (f fingerprint) {\n", exprType)

	fmt.Fprintf(g.w, "  const size = unsafe.Sizeof(%s{})\n", exprType)
	fmt.Fprintf(g.w, "  const offset = unsafe.Offsetof(%s{}.op)\n\n", exprType)

	fmt.Fprintf(g.w, "  b := *(*[size]byte)(unsafe.Pointer(e))\n\n")

	// If size is less than or equal to the size of Fingerprint, then
	// inline the fields in the fingerprint. Otherwise, use MD5 hash.
	fmt.Fprintf(g.w, "  if size - offset <= unsafe.Sizeof(f) {\n")
	fmt.Fprintf(g.w, "    copy(f[:], b[offset:])\n")
	fmt.Fprintf(g.w, "  } else {\n")
	fmt.Fprintf(g.w, "    f = fingerprint(md5.Sum(b[offset:]))\n")
	fmt.Fprintf(g.w, "  }\n\n")

	fmt.Fprintf(g.w, "  return\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *exprsGen) genMemoFuncs(define *DefineExpr) {
	opType := fmt.Sprintf("%sOp", define.Name())
	exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

	// Generate a conversion method from memoExpr to the more specialized
	// expression type.
	fmt.Fprintf(g.w, "func (m *memoExpr) as%s() *%s {\n", define.Name(), exprType)
	fmt.Fprintf(g.w, "  if m.op != %s {\n", opType)
	fmt.Fprintf(g.w, "    return nil\n")
	fmt.Fprintf(g.w, "  }\n\n")

	fmt.Fprintf(g.w, "  return (*%s)(unsafe.Pointer(m))\n", exprType)
	fmt.Fprintf(g.w, "}\n\n")

	// Generate a method on the memo class that memoizes an expression of
	// the currently generating type.
	fmt.Fprintf(g.w, "func (m *memo) memoize%s(expr *%s) GroupID {\n", define.Name(), exprType)
	fmt.Fprintf(g.w, "  const size = uint32(unsafe.Sizeof(%s{}))\n", exprType)
	fmt.Fprintf(g.w, "  const align = uint32(unsafe.Alignof(%s{}))\n\n", exprType)

	for _, elem2 := range define.Fields() {
		field := elem2.(*DefineFieldExpr)
		if field.IsListType() {
			fmt.Fprintf(g.w, "  if expr.%s == UndefinedList {\n", unTitle(field.Name()))
		} else {
			fmt.Fprintf(g.w, "  if expr.%s == 0 {\n", unTitle(field.Name()))
		}

		fmt.Fprintf(g.w, "    panic(\"%s child cannot be undefined\")\n", unTitle(field.Name()))
		fmt.Fprintf(g.w, "  }\n\n")
	}

	fmt.Fprintf(g.w, "  fingerprint := expr.fingerprint()\n")
	fmt.Fprintf(g.w, "  loc := m.exprMap[fingerprint]\n")
	fmt.Fprintf(g.w, "  if loc.offset == 0 {\n")
	fmt.Fprintf(g.w, "    loc.offset = exprOffset(m.arena.alloc(size, align))\n")
	fmt.Fprintf(g.w, "    p := (*%s)(m.arena.getPointer(uint32(loc.offset)))\n", exprType)
	fmt.Fprintf(g.w, "    *p = *expr\n\n")

	fmt.Fprintf(g.w, "    if loc.group == 0 {\n")
	fmt.Fprintf(g.w, "      if expr.group != 0 {\n")
	fmt.Fprintf(g.w, "        loc.group = expr.group")
	fmt.Fprintf(g.w, "      } else {\n")
	fmt.Fprintf(g.w, "        mgrp := m.newGroup(%s, loc.offset)\n", opType)
	fmt.Fprintf(g.w, "        p.group = mgrp.id\n")
	fmt.Fprintf(g.w, "        loc.group = mgrp.id\n")
	fmt.Fprintf(g.w, "        e := Expr{mem: m, group: mgrp.id, op: %s, offset: loc.offset}\n", opType)
	fmt.Fprintf(g.w, "        mgrp.logical = m.logPropsFactory.constructProps(&e)\n")
	fmt.Fprintf(g.w, "      }\n")
	fmt.Fprintf(g.w, "    } else {\n")
	fmt.Fprintf(g.w, "      if expr.group != loc.group {\n")
	fmt.Fprintf(g.w, "        panic(\"denormalized expression's group doesn't match fingerprint group\")\n")
	fmt.Fprintf(g.w, "      }\n")
	fmt.Fprintf(g.w, "    }\n\n")

	fmt.Fprintf(g.w, "    m.lookupGroup(loc.group).addExpr(loc.offset)\n")
	fmt.Fprintf(g.w, "    m.exprMap[fingerprint] = loc\n")
	fmt.Fprintf(g.w, "  }\n\n")

	fmt.Fprintf(g.w, "  return loc.group\n")
	fmt.Fprintf(g.w, "}\n\n")
}
