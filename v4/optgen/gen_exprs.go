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
	fmt.Fprintf(w, "type childCountLookupFunc func(e *expr) int\n")

	fmt.Fprintf(w, "var childCountLookup = []childCountLookupFunc{\n")
	fmt.Fprintf(w, "  nil,\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		fmt.Fprintf(w, "  // %sOp\n", unTitle(define.Name()))
		fmt.Fprintf(w, "  func(e *expr) int {\n")

		count := len(define.Fields())
		if define.PrivateField() != nil {
			count--
		}

		list := define.ListField()
		if list != nil {
			fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))\n", exprType, exprType)
			fmt.Fprintf(w, "    return %d + int(%s.%s.len)\n", count-1, exprType, unTitle(list.Name()))
		} else {
			fmt.Fprintf(w, "    return %d\n", count)
		}

		fmt.Fprintf(w, "  },\n\n")
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate input group lookup table.
	fmt.Fprintf(w, "type childGroupLookupFunc func(e *expr, n int) groupID\n")

	fmt.Fprintf(w, "var childGroupLookup = []childGroupLookupFunc{\n")
	fmt.Fprintf(w, "  nil, // unknownOp\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		fmt.Fprintf(w, "  // %sOp\n", unTitle(define.Name()))
		fmt.Fprintf(w, "  func(e *expr, n int) groupID {\n")

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

		fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))\n\n", exprType, exprType)

		fmt.Fprintf(w, "  switch n {\n")

		for index, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsPrivateType() {
				// Don't include private field.
				break
			}

			if field.IsListType() {
				fmt.Fprintf(w, "  default:\n")
				fmt.Fprintf(w, "    list := e.memo.lookupList(%s.%s)\n", exprType, unTitle(field.Name()))
				fmt.Fprintf(w, "    return list[n - %d]", index)
				fmt.Fprintf(w, "  }\n")
				break
			}

			fmt.Fprintf(w, "  case %d:\n", index)
			fmt.Fprintf(w, "    return %s.%s\n", exprType, unTitle(field.Name()))
		}

		if define.ListField() == nil {
			fmt.Fprintf(w, "  default:\n")
			fmt.Fprintf(w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(w, "  }\n")
		}

		fmt.Fprintf(w, "  },\n\n")
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate provided props lookup table.
	fmt.Fprintf(w, "type providedPropsLookupFunc func(e *expr) physicalPropsID\n")

	fmt.Fprintf(w, "var providedPropsLookup = []providedPropsLookupFunc{\n")
	fmt.Fprintf(w, "  nil, // unknownOp\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		if define.HasTag("ProvidedProps") {
			fmt.Fprintf(w, "  // %sOp\n", unTitle(define.Name()))
			fmt.Fprintf(w, "  func(e *expr) physicalPropsID {\n")
			fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))\n", exprType, exprType)
			fmt.Fprintf(w, "    return %s.computeProvidedProps(e.memo, e.required)\n", exprType)
			fmt.Fprintf(w, "  },\n\n")
		} else {
			fmt.Fprintf(w, "  // %sOp\n", unTitle(define.Name()))
			fmt.Fprintf(w, "  defaultProvidedProps,\n\n")
		}
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate required props lookup table.
	fmt.Fprintf(w, "type requiredPropsLookupFunc func(e *expr, nth int) physicalPropsID\n")

	fmt.Fprintf(w, "var requiredPropsLookup = []requiredPropsLookupFunc{\n")
	fmt.Fprintf(w, "  nil, // unknownOp\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		if define.HasTag("RequiredProps") {
			fmt.Fprintf(w, "  // %sOp\n", unTitle(define.Name()))
			fmt.Fprintf(w, "  func(e *expr, nth int) physicalPropsID {\n")
			fmt.Fprintf(w, "    %s := (*%s)(unsafe.Pointer(e.memo.lookupExpr(e.offset)))\n", exprType, exprType)
			fmt.Fprintf(w, "    return %s.computeRequiredProps(e.memo, e.required, nth)\n", exprType)
			fmt.Fprintf(w, "  },\n\n")
		} else {
			fmt.Fprintf(w, "  // %sOp\n", unTitle(define.Name()))
			fmt.Fprintf(w, "  defaultRequiredProps,\n\n")
		}
	}

	fmt.Fprintf(w, "}\n\n")

	// Generate lookup tables that indicate whether an expression is associated
	// with a particular tag.
	for _, tag := range g.compiled.DefinitionTags() {
		fmt.Fprintf(w, "var is%sLookup = []bool{\n", tag)
		fmt.Fprintf(w, "  false, // unknownOp\n\n")

		for _, elem := range g.compiled.Root().Defines().All() {
			define := elem.(*DefineExpr)
			fmt.Fprintf(w, "  %v, // %sOp\n", define.HasTag(tag), unTitle(define.Name()))
		}

		fmt.Fprintf(w, "}\n\n")
	}

	// Add isXXX() tag methods on expr for each definition tag.
	for _, tag := range g.compiled.DefinitionTags() {
		fmt.Fprintf(w, "func (e *expr) is%s() bool{\n", tag)
		fmt.Fprintf(w, "  return is%sLookup[e.op]\n", tag)
		fmt.Fprintf(w, "}\n\n")
	}

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		opType := fmt.Sprintf("%sOp", unTitle(define.Name()))
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

		// Generate the fingerprint method.
		fmt.Fprintf(w, "func (e *%s) fingerprint() (f exprFingerprint) {\n", exprType)

		fmt.Fprintf(w, "  const size = unsafe.Sizeof(%s{})\n", exprType)
		fmt.Fprintf(w, "  const offset = unsafe.Offsetof(%s{}.op)\n\n", exprType)

		fmt.Fprintf(w, "  b := *(*[size]byte)(unsafe.Pointer(e))\n\n")

		// If size is less than or equal to the size of exprFingerprint, then
		// inline the fields in the fingerprint. Otherwise, use MD5 hash.
		fmt.Fprintf(w, "  if size - offset <= unsafe.Sizeof(f) {\n")
		fmt.Fprintf(w, "    copy(f[:], b[offset:])\n")
		fmt.Fprintf(w, "  } else {\n")
		fmt.Fprintf(w, "    f = exprFingerprint(md5.Sum(b[offset:]))\n")
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
		fmt.Fprintf(w, "func (m *memo) memoize%s(e *%s) groupID {\n", define.Name(), exprType)
		fmt.Fprintf(w, "  const size = uint32(unsafe.Sizeof(%s{}))\n", exprType)
		fmt.Fprintf(w, "  const align = uint32(unsafe.Alignof(%s{}))\n\n", exprType)

		fmt.Fprintf(w, "  fingerprint := e.fingerprint()\n")
		fmt.Fprintf(w, "  loc := m.exprMap[fingerprint]\n")
		fmt.Fprintf(w, "  if loc.offset == 0 {\n")
		fmt.Fprintf(w, "    loc.offset = exprOffset(m.arena.Alloc(size, align))\n\n")

		fmt.Fprintf(w, "    if loc.group == 0 {\n")
		fmt.Fprintf(w, "      if e.group != 0 {\n")
		fmt.Fprintf(w, "        loc.group = e.group")
		fmt.Fprintf(w, "      } else {\n")
		fmt.Fprintf(w, "        loc.group = m.newGroup(e, loc.offset)\n")
		fmt.Fprintf(w, "      }\n")
		fmt.Fprintf(w, "    } else {\n")
		fmt.Fprintf(w, "      if e.group != loc.group {\n")
		fmt.Fprintf(w, "        panic(\"denormalized expression's group doesn't match fingerprint group\")\n")
		fmt.Fprintf(w, "      }\n")
		fmt.Fprintf(w, "    }\n\n")

		fmt.Fprintf(w, "    p := (*%s)(m.arena.GetPointer(uint32(loc.offset)))\n", exprType)
		fmt.Fprintf(w, "    *p = *e\n\n")
		fmt.Fprintf(w, "    p.group = loc.group\n\n")

		fmt.Fprintf(w, "    m.lookupGroup(loc.group).addExpr(loc.offset)\n")
		fmt.Fprintf(w, "    m.exprMap[fingerprint] = loc\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  return loc.group\n")
		fmt.Fprintf(w, "}\n\n")
	}
}
