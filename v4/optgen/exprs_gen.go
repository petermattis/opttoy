package optgen

import (
	"fmt"
	"io"
)

type ExprsGen struct {
	compiled CompiledExpr
	w        io.Writer
}

func (g *ExprsGen) Generate(compiled CompiledExpr, w io.Writer) {
	g.compiled = compiled
	g.w = w

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

func (g *ExprsGen) genChildCountLookup() {
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
			fmt.Fprintf(g.w, "    %s := (*%s)(e.mem.lookupExpr(e.loc))\n", varName, exprType)
			fmt.Fprintf(g.w, "    return %d + int(%s.%s().len)\n", count-1, varName, unTitle(list.Name()))
		} else {
			fmt.Fprintf(g.w, "    return %d\n", count)
		}

		fmt.Fprintf(g.w, "  },\n\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *ExprsGen) genChildGroupLookup() {
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
			fmt.Fprintf(g.w, "      return e.loc.group\n")
			fmt.Fprintf(g.w, "    }\n\n")

			fmt.Fprintf(g.w, "    panic(\"child index out of range\")\n")
			fmt.Fprintf(g.w, "  },\n\n")
			continue
		}

		fmt.Fprintf(g.w, "    %s := (*%s)(e.mem.lookupExpr(e.loc))\n\n", varName, exprType)

		fmt.Fprintf(g.w, "  switch n {\n")

		for index, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsPrivateType() {
				// Don't include private field.
				break
			}

			if field.IsListType() {
				fmt.Fprintf(g.w, "  default:\n")
				fmt.Fprintf(g.w, "    list := e.mem.lookupList(%s.%s())\n", varName, unTitle(field.Name()))
				fmt.Fprintf(g.w, "    return list[n - %d]", index)
				fmt.Fprintf(g.w, "  }\n")
				break
			}

			fmt.Fprintf(g.w, "  case %d:\n", index)
			fmt.Fprintf(g.w, "    return %s.%s()\n", varName, unTitle(field.Name()))
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

func (g *ExprsGen) genPrivateFieldLookup() {
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
			fmt.Fprintf(g.w, "    %s := (*%s)(e.mem.lookupExpr(e.loc))\n", varName, exprType)
			fmt.Fprintf(g.w, "    return %s.%s()\n", varName, unTitle(private.Name()))
		} else {
			fmt.Fprintf(g.w, "    return 0\n")
		}

		fmt.Fprintf(g.w, "  },\n\n")
	}

	fmt.Fprintf(g.w, "}\n\n")
}

func (g *ExprsGen) genTagLookup() {
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

func (g *ExprsGen) genIsTag() {
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

func (g *ExprsGen) genExprFuncs(define *DefineExpr) {
	opType := fmt.Sprintf("%sOp", define.Name())
	exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

	// Generate the expression type.
	fmt.Fprintf(g.w, "type %s memoExpr\n\n", exprType)

	// Generate a strongly-typed constructor function for the type.
	fmt.Fprintf(g.w, "func make%sExpr(", define.Name())
	for i, elem2 := range define.Fields() {
		field := elem2.(*DefineFieldExpr)
		if i != 0 {
			fmt.Fprint(g.w, ", ")
		}
		fmt.Fprintf(g.w, "%s %s", unTitle(field.Name()), mapType(field.Type()))
	}
	fmt.Fprintf(g.w, ") %s {\n", exprType)
	fmt.Fprintf(g.w, "  return %s{op: %s, state: exprState{", exprType, opType)

	for i, elem2 := range define.Fields() {
		field := elem2.(*DefineFieldExpr)
		fieldName := unTitle(field.Name())

		if i != 0 {
			fmt.Fprintf(g.w, ", ")
		}

		if field.IsListType() {
			fmt.Fprintf(g.w, "%s.offset, %s.len", fieldName, fieldName)
		} else {
			fmt.Fprintf(g.w, "uint32(%s)", fieldName)
		}
	}

	fmt.Fprint(g.w, "}}\n")
	fmt.Fprint(g.w, "}\n\n")

	// Generate the strongly-typed accessor methods.
	stateIndex := 0
	for _, elem2 := range define.Fields() {
		field := elem2.(*DefineFieldExpr)

		fmt.Fprintf(g.w, "func (e *%s) %s() %s {\n", exprType, unTitle(field.Name()), mapType(field.Type()))
		if field.IsListType() {
			fmt.Fprintf(g.w, "  return ListID{offset: e.state[%d], len: e.state[%d]}\n", stateIndex, stateIndex+1)
			stateIndex += 2
		} else if field.IsPrivateType() {
			fmt.Fprintf(g.w, "  return PrivateID(e.state[%d])\n", stateIndex)
			stateIndex++
		} else {
			fmt.Fprintf(g.w, "  return GroupID(e.state[%d])\n", stateIndex)
			stateIndex++
		}
		fmt.Fprintf(g.w, "}\n\n")
	}

	// Generate the fingerprint method.
	fmt.Fprintf(g.w, "func (e *%s) fingerprint() fingerprint {\n", exprType)
	fmt.Fprintf(g.w, "  return fingerprint(*e)\n")
	fmt.Fprintf(g.w, "}\n\n")
}

func (g *ExprsGen) genMemoFuncs(define *DefineExpr) {
	opType := fmt.Sprintf("%sOp", define.Name())
	exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))

	// Generate a conversion method from memoExpr to the more specialized
	// expression type.
	fmt.Fprintf(g.w, "func (m *memoExpr) as%s() *%s {\n", define.Name(), exprType)
	fmt.Fprintf(g.w, "  if m.op != %s {\n", opType)
	fmt.Fprintf(g.w, "    return nil\n")
	fmt.Fprintf(g.w, "  }\n")

	fmt.Fprintf(g.w, "  return (*%s)(m)\n", exprType)
	fmt.Fprintf(g.w, "}\n\n")
}
