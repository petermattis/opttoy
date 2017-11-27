package main

import (
	"fmt"
	"io"
)

const fingerprintSize = 16

func (g *generator) generateExprs(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	fmt.Fprintf(w, "import (\n")
	fmt.Fprintf(w, "  \"crypto/md5\"\n")
	fmt.Fprintf(w, "  \"unsafe\"\n")
	fmt.Fprintf(w, ")\n\n")

	fmt.Fprintf(w, "type expr interface {\n")
	fmt.Fprintf(w, "  fingerprint() exprFingerprint\n")
	fmt.Fprintf(w, "  operator() operator\n")
	fmt.Fprintf(w, "  childCount(m *memo) int\n")
	fmt.Fprintf(w, "  child(m *memo, n int) groupID\n")
	fmt.Fprintf(w, "  private(m *memo) interface{}\n")
	fmt.Fprintf(w, "  logicalProps(m *memo) *logicalProps\n\n")

	for _, tag := range g.compiled.DefinitionTags() {
		fmt.Fprintf(w, "  is%s() bool\n", tag)
	}

	fmt.Fprintf(w, "}\n\n")

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)
		opType := fmt.Sprintf("%sOp", unTitle(define.Name()))
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		// Generate the expression struct.
		fmt.Fprintf(w, "type %s struct {\n", exprType)
		fmt.Fprintf(w, "  group groupID\n")
		fmt.Fprintf(w, "  op    operator\n")

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

		// Generate the expr interface's operator method.
		fmt.Fprintf(w, "func (e *%s) operator() operator {\n", exprType)
		fmt.Fprintf(w, "  return %s\n", opType)
		fmt.Fprintf(w, "}\n\n")

		count := len(define.Fields())
		if define.Private() != nil {
			count--
		}

		// Generate the expr interface's childCount method.
		fmt.Fprintf(w, "func (e *%s) childCount(m *memo) int {\n", exprType)

		list := define.ListField()
		if list != nil {
			fmt.Fprintf(w, "  return %d + int(e.%s.len)\n", count-1, unTitle(list.Name()))
		} else {
			fmt.Fprintf(w, "  return %d\n", count)
		}

		fmt.Fprintf(w, "}\n\n")

		// Generate the expr interface's child method.
		fmt.Fprintf(w, "func (e *%s) child(m *memo, n int) groupID {\n", exprType)
		fmt.Fprintf(w, "  switch n {\n")

		for index, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsPrivateType() {
				// Don't include private field.
				break
			}

			if field.IsListType() {
				fmt.Fprintf(w, "  default:\n")
				fmt.Fprintf(w, "    list := m.lookupList(e.%s)\n", unTitle(field.Name()))
				fmt.Fprintf(w, "    return list[n - %d]", index)
				fmt.Fprintf(w, "  }\n")
				break
			}

			fmt.Fprintf(w, "  case %d:\n", index)
			fmt.Fprintf(w, "    return e.%s\n", unTitle(field.Name()))
		}

		if define.ListField() == nil {
			fmt.Fprintf(w, "  default:\n")
			fmt.Fprintf(w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(w, "  }\n")
		}

		fmt.Fprintf(w, "}\n\n")

		// Generate the expr interface's private method.
		fmt.Fprintf(w, "func (e *%s) private(m *memo) interface{} {\n", exprType)

		if define.Private() != nil {
			fmt.Fprintf(w, "  return m.lookupPrivate(e.%s)\n", unTitle(define.PrivateField().Name()))
		} else {
			fmt.Fprintf(w, "  return nil\n")
		}

		fmt.Fprintf(w, "}\n\n")

		// Generate the expr interface's logicalProps method.
		fmt.Fprintf(w, "func (e *%s) logicalProps(m *memo) *logicalProps {\n", exprType)
		fmt.Fprintf(w, "  return m.lookupGroup(e.group).props\n")
		fmt.Fprintf(w, "}\n\n")

		// Generate the methods that check whether an expression is associated
		// with a particular tag.
		for _, tag := range g.compiled.DefinitionTags() {
			fmt.Fprintf(w, "func (e *%s) is%s() bool {\n", exprType, tag)

			found := false
			for _, elem2 := range define.Tags().All() {
				s := elem2.(*StringExpr)
				if s.Value() == tag {
					found = true
					break
				}
			}

			fmt.Fprintf(w, "  return %v\n", found)
			fmt.Fprintf(w, "}\n\n")
		}

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

		offsetName := fmt.Sprintf("%sOffset", unTitle(define.Name()))
		fmt.Fprintf(w, "  %s := m.lookupExprByFingerprint(e.fingerprint())\n", offsetName)
		fmt.Fprintf(w, "  if %s != 0 {\n", offsetName)
		fmt.Fprintf(w, "    return m.lookupExpr(%s).group\n", offsetName)
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  offset := m.arena.Alloc(size, align)\n\n")

		fmt.Fprintf(w, "  if e.group == 0 {\n")
		fmt.Fprintf(w, "    e.group = m.newGroup(e, exprOffset(offset))\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  m.lookupGroup(e.group).addExpr(exprOffset(offset))\n\n")

		fmt.Fprintf(w, "  p := (*%s)(m.arena.GetPointer(offset))\n", exprType)
		fmt.Fprintf(w, "  *p = *e\n\n")

		fmt.Fprintf(w, "  return e.group\n")
		fmt.Fprintf(w, "}\n\n")
	}
}
