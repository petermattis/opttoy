package main

import (
	"fmt"
	"io"
)

const fingerprintSize = 16

func (g *generator) generateExprs(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	fmt.Fprintf(w, "import (\n")
	fmt.Fprintf(w, "  \"unsafe\"\n")
	fmt.Fprintf(w, ")\n\n")

	for _, elem := range g.root.Defines().All() {
		define := elem.AsDefine()
		opType := fmt.Sprintf("%sOp", unTitle(define.Name()))
		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

		fmt.Fprintf(w, "type %s struct {\n", exprType)
		fmt.Fprintf(w, "  group groupID\n")
		fmt.Fprintf(w, "  op    operator\n")

		for _, elem2 := range define.Fields() {
			field := elem2.AsDefineField()
			fmt.Fprintf(w, "  %s %s\n", unTitle(field.Name()), mapType(field.Type()))
		}

		fmt.Fprintf(w, "}\n\n")

		fmt.Fprintf(w, "func (e *%s) fingerprint() (f exprFingerprint) {\n", exprType)

		fmt.Fprintf(w, "  const size = unsafe.Sizeof(%s{})\n", exprType)
		fmt.Fprintf(w, "  const offset = unsafe.Offsetof(%s{}.op)\n\n", exprType)

		fmt.Fprintf(w, "  b := ([size]byte)(unsafe.Pointer(e))\n\n")

		// If size is less than or equal to the size of exprFingerprint, then
		// inline the fields in the fingerprint. Otherwise, use MD5 hash.
		fmt.Fprintf(w, "  if size - offset <= unsafe.Sizeof(f) {\n")
		fmt.Fprintf(w, "    copy(f[:], b[offset:])\n")
		fmt.Fprintf(w, "  } else {\n")
		fmt.Fprintf(w, "    f = exprFingerprint(md5.Sum(b[offset:]))\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  return\n")
		fmt.Fprintf(w, "}\n\n")

		fmt.Fprintf(w, "func (e *%s) op() operator {\n", exprType)
		fmt.Fprintf(w, "  return %s\n", opType)
		fmt.Fprintf(w, "}\n\n")

		count := len(define.Fields())
		if define.Private() != nil {
			count--
		}

		fmt.Fprintf(w, "func (e *%s) childCount(m *memo) int {\n", exprType)

		list := define.List()
		if list != nil {
			fmt.Fprintf(w, "  return %d + len(m.lookupList(e.%s))\n", count-1, unTitle(list.Name()))
		} else {
			fmt.Fprintf(w, "  return %d\n", count)
		}

		fmt.Fprintf(w, "}\n\n")

		fmt.Fprintf(w, "func (e *%s) child(m *memo, n int) baseExpr {\n", exprType)
		fmt.Fprintf(w, "  switch n {\n")

		for index, elem2 := range define.Fields() {
			field := elem2.AsDefineField()
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
			fmt.Fprintf(w, "    return m.lookupExpr(e.%s).asBaseExpr()\n", unTitle(field.Name()))
		}

		if define.List() == nil {
			fmt.Fprintf(w, "  default:\n")
			fmt.Fprintf(w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(w, "  }\n")
		}

		fmt.Fprintf(w, "}\n\n")

		fmt.Fprintf(w, "func (e *%s) private(m *memo) interface{} {\n", exprType)

		if define.Private() != nil {
			fmt.Fprintf(w, "  return m.lookupPrivate[e.%s]\n", unTitle(define.Private().Name()))
		} else {
			fmt.Fprintf(w, "  return nil\n")
		}

		fmt.Fprintf(w, "}\n\n")

		fmt.Fprintf(w, "func (m *memoExpr) as%s() *%s {\n", define.Name(), exprType)
		fmt.Fprintf(w, "  if m.op != %s {\n", opType)
		fmt.Fprintf(w, "    return nil\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  return (*%s)(unsafe.Pointer(m))\n", exprType)
		fmt.Fprintf(w, "}\n\n")

		fmt.Fprintf(w, "func (m *memo) memoize%s(e *%s) exprOffset {\n", define.Name(), exprType)
		fmt.Fprintf(w, "  const size = uint32(unsafe.Sizeof(%s{}))\n", exprType)
		fmt.Fprintf(w, "  const align = uint32(unsafe.Alignof(%s{}))\n\n", exprType)

		offsetName := fmt.Sprintf("%sOffset", unTitle(define.Name()))
		fmt.Fprintf(w, "  %s := m.lookupExprByFingerprint(e.fingerprint())\n", offsetName)
		fmt.Fprintf(w, "  if %s != 0 {\n", offsetName)
		fmt.Fprintf(w, "    return %s\n", offsetName)
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  if e.group == 0 {\n")
		fmt.Fprintf(w, "    e.group = m.newGroup(e.createLogicalProps())\n")
		fmt.Fprintf(w, "  }\n\n")

		fmt.Fprintf(w, "  offset = m.arena.Alloc(size, align)\n")
		fmt.Fprintf(w, "  p := (*%s)(m.arena.GetPointer(offset))\n", exprType)
		fmt.Fprintf(w, "  *p = *e\n\n")

		fmt.Fprintf(w, "  return exprOffset(offset)\n")
		fmt.Fprintf(w, "}\n\n")
	}
}
