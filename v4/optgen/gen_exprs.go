package main

import (
	"fmt"
	"io"
)

func (g *generator) genExprs(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	fmt.Fprintf(w, "import (\n")
	fmt.Fprintf(w, "  \"crypto/md5\"\n")
	fmt.Fprintf(w, "  \"unsafe\"\n")
	fmt.Fprintf(w, ")\n\n")

	// Generate child count lookup table.
	fmt.Fprintf(w, "type childCountLookupFunc func(e *Expr) int\n")

	fmt.Fprintf(w, "var childCountLookup = []childCountLookupFunc{\n")
	fmt.Fprintf(w, "  nil,\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		varName := exprType

		fmt.Fprintf(w, "  // %sOp\n", define.Name())
		fmt.Fprintf(w, "  func(e *Expr) int {\n")

		count := len(define.Fields())
		if define.PrivateField() != nil {
			count--
		}

		list := define.ListField()
		if list != nil {
			fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.mem.lookupExpr(e.offset)))\n", varName, exprType)
			fmt.Fprintf(w, "    return %d + int(%s.%s.len)\n", count-1, varName, unTitle(list.Name()))
		} else {
			fmt.Fprintf(w, "    return %d\n", count)
		}

		fmt.Fprintf(w, "  },\n\n")
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate input group lookup table.
	fmt.Fprintf(w, "type childGroupLookupFunc func(e *Expr, n int) GroupID\n")

	fmt.Fprintf(w, "var childGroupLookup = []childGroupLookupFunc{\n")
	fmt.Fprintf(w, "  nil, // unknownOp\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		varName := exprType

		fmt.Fprintf(w, "  // %sOp\n", define.Name())
		fmt.Fprintf(w, "  func(e *Expr, n int) GroupID {\n")

		count := len(define.Fields())
		if define.PrivateField() != nil {
			count--
		}

		if count == 0 {
			fmt.Fprintf(w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(w, "  },\n\n")
			continue
		}

		if define.HasTag("Enforcer") {
			// Enforcers have a single child which is the same group they're in.
			fmt.Fprintf(w, "    if n == 0 {\n")
			fmt.Fprintf(w, "      return e.group\n")
			fmt.Fprintf(w, "    }\n\n")

			fmt.Fprintf(w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(w, "  },\n\n")
			continue
		}

		fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.mem.lookupExpr(e.offset)))\n\n", varName, exprType)

		fmt.Fprintf(w, "  switch n {\n")

		for index, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsPrivateType() {
				// Don't include private field.
				break
			}

			if field.IsListType() {
				fmt.Fprintf(w, "  default:\n")
				fmt.Fprintf(w, "    list := e.mem.lookupList(%s.%s)\n", varName, unTitle(field.Name()))
				fmt.Fprintf(w, "    return list[n - %d]", index)
				fmt.Fprintf(w, "  }\n")
				break
			}

			fmt.Fprintf(w, "  case %d:\n", index)
			fmt.Fprintf(w, "    return %s.%s\n", varName, unTitle(field.Name()))
		}

		if define.ListField() == nil {
			fmt.Fprintf(w, "  default:\n")
			fmt.Fprintf(w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(w, "  }\n")
		}

		fmt.Fprintf(w, "  },\n\n")
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate private field lookup table.
	fmt.Fprintf(w, "type privateIDLookupFunc func(e *Expr) PrivateID\n")

	fmt.Fprintf(w, "var privateIDLookup = []privateIDLookupFunc{\n")
	fmt.Fprintf(w, "  nil,\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		varName := unTitle(exprType)

		fmt.Fprintf(w, "  // %sOp\n", define.Name())
		fmt.Fprintf(w, "  func(e *Expr) PrivateID {\n")

		private := define.PrivateField()
		if private != nil {
			fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.mem.lookupExpr(e.offset)))\n", varName, exprType)
			fmt.Fprintf(w, "    return %s.%s\n", varName, unTitle(private.Name()))
		} else {
			fmt.Fprintf(w, "    panic(\"expression does not have a private field\")\n")
		}

		fmt.Fprintf(w, "  },\n\n")
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate lookup tables that indicate whether an expression is associated
	// with a particular tag.
	for _, tag := range g.compiled.DefinitionTags() {
		fmt.Fprintf(w, "var is%sLookup = []bool{\n", tag)
		fmt.Fprintf(w, "  false, // UnknownOp\n\n")

		for _, elem := range g.compiled.Root().Defines().All() {
			define := elem.(*DefineExpr)
			fmt.Fprintf(w, "  %v, // %sOp\n", define.HasTag(tag), define.Name())
		}

		fmt.Fprintf(w, "}\n\n")
	}

	// Add isXXX() tag methods on expr for each definition tag.
	for _, tag := range g.compiled.DefinitionTags() {
		fmt.Fprintf(w, "func (e *Expr) Is%s() bool {\n", tag)
		fmt.Fprintf(w, "  return is%sLookup[e.op]\n", tag)
		fmt.Fprintf(w, "}\n\n")
	}

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		opType := fmt.Sprintf("%sOp", define.Name())
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		// Skip enforcers, since they are not memoized.
		if define.HasTag("Enforcer") {
			continue
		}

		// Generate the expression struct.
		fmt.Fprintf(w, "type %s struct {\n", exprType)
		fmt.Fprintf(w, "  memoExpr\n")

		for _, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			fmt.Fprintf(w, "  %s %s\n", unTitle(field.Name()), mapType(field.Type()))
		}

		fmt.Fprintf(w, "}\n\n")

		// Generate the Fingerprint method.
		fmt.Fprintf(w, "func (e *%s) fingerprint() (f fingerprint) {\n", exprType)

		fmt.Fprintf(w, "  const size = unsafe.Sizeof(%s{})\n", exprType)
		fmt.Fprintf(w, "  const offset = unsafe.Offsetof(%s{}.op)\n\n", exprType)

		fmt.Fprintf(w, "  b := *(*[size]byte)(unsafe.Pointer(e))\n\n")

		// If size is less than or equal to the size of Fingerprint, then
		// inline the fields in the fingerprint. Otherwise, use MD5 hash.
		fmt.Fprintf(w, "  if size - offset <= unsafe.Sizeof(f) {\n")
		fmt.Fprintf(w, "    copy(f[:], b[offset:])\n")
		fmt.Fprintf(w, "  } else {\n")
		fmt.Fprintf(w, "    f = fingerprint(md5.Sum(b[offset:]))\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  return\n")
		fmt.Fprintf(w, "}\n\n")

		// Generate a conversion method from memoExpr to the more specialized
		// expression type.
		fmt.Fprintf(w, "func (m *memoExpr) as%s() *%s {\n", define.Name(), exprType)
		fmt.Fprintf(w, "  if m.op != %s {\n", opType)
		fmt.Fprintf(w, "    return nil\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  return (*%s)(unsafe.Pointer(m))\n", exprType)
		fmt.Fprintf(w, "}\n\n")

		// Generate a method on the memo class that memoizes an expression of
		// the currently generating type.
		fmt.Fprintf(w, "func (m *memo) memoize%s(expr *%s) GroupID {\n", define.Name(), exprType)
		fmt.Fprintf(w, "  const size = uint32(unsafe.Sizeof(%s{}))\n", exprType)
		fmt.Fprintf(w, "  const align = uint32(unsafe.Alignof(%s{}))\n\n", exprType)

		for _, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsListType() {
				fmt.Fprintf(w, "  if expr.%s == UndefinedList {\n", unTitle(field.Name()))
			} else {
				fmt.Fprintf(w, "  if expr.%s == 0 {\n", unTitle(field.Name()))
			}

			fmt.Fprintf(w, "    panic(\"%s child cannot be undefined\")\n", unTitle(field.Name()))
			fmt.Fprintf(w, "  }\n\n")
		}

		fmt.Fprintf(w, "  fingerprint := expr.fingerprint()\n")
		fmt.Fprintf(w, "  loc := m.exprMap[fingerprint]\n")
		fmt.Fprintf(w, "  if loc.offset == 0 {\n")
		fmt.Fprintf(w, "    loc.offset = exprOffset(m.arena.alloc(size, align))\n")
		fmt.Fprintf(w, "    p := (*%s)(m.arena.getPointer(uint32(loc.offset)))\n", exprType)
		fmt.Fprintf(w, "    *p = *expr\n\n")

		fmt.Fprintf(w, "    if loc.group == 0 {\n")
		fmt.Fprintf(w, "      if expr.group != 0 {\n")
		fmt.Fprintf(w, "        loc.group = expr.group")
		fmt.Fprintf(w, "      } else {\n")
		fmt.Fprintf(w, "        mgrp := m.newGroup(%s, loc.offset)\n", opType)
		fmt.Fprintf(w, "        p.group = mgrp.id\n")
		fmt.Fprintf(w, "        loc.group = mgrp.id\n")
		fmt.Fprintf(w, "        e := Expr{mem: m, group: mgrp.id, op: %s, offset: loc.offset}\n", opType)
		fmt.Fprintf(w, "        mgrp.logical = m.logPropsFactory.constructProps(&e)\n")
		fmt.Fprintf(w, "      }\n")
		fmt.Fprintf(w, "    } else {\n")
		fmt.Fprintf(w, "      if expr.group != loc.group {\n")
		fmt.Fprintf(w, "        panic(\"denormalized expression's group doesn't match fingerprint group\")\n")
		fmt.Fprintf(w, "      }\n")
		fmt.Fprintf(w, "    }\n\n")

		fmt.Fprintf(w, "    m.lookupGroup(loc.group).addExpr(loc.offset)\n")
		fmt.Fprintf(w, "    m.exprMap[fingerprint] = loc\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  return loc.group\n")
		fmt.Fprintf(w, "}\n\n")
	}
}
