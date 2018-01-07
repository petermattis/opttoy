package optgen

import (
	"fmt"
	"io"
)

type FactoryGen struct {
	xformGen
}

func (g *FactoryGen) Generate(compiled CompiledExpr, w io.Writer) {
	g.init(compiled, w, "Normalize")

	for _, define := range g.defines {
		g.resetUnique()

		g.w.writeIndent("func (_f *Factory) Construct%s(\n", define.name)

		for _, field := range define.fields {
			g.w.writeIndent("  %s %s,\n", field.name, field.typ)
		}

		g.w.nest(") GroupID {\n")

		g.w.writeIndent("%s := %s{memoExpr: memoExpr{op: %s}", define.varName, define.exprType, define.opType)

		for _, field := range define.fields {
			g.w.write(", %s: %s", field.name, field.name)
		}

		g.w.write("}\n")
		g.w.writeIndent("_fingerprint := %s.fingerprint()\n", define.varName)
		g.w.writeIndent("_group := _f.mem.lookupGroupByFingerprint(_fingerprint)\n")
		g.w.nest("if _group != 0 {\n")
		g.w.writeIndent("return _group\n")
		g.w.unnest(1, "}\n\n")

		if len(define.rules) > 0 {
			g.w.nest("if _f.maxSteps <= 0 {\n")
			g.w.writeIndent("return _f.mem.memoize%s(&%s)\n", define.name, define.varName)
			g.w.unnest(1, "}\n\n")
		}

		for _, rule := range define.rules {
			g.genRule(rule)
		}

		if len(define.rules) > 0 {
			g.w.write("\n")
		}

		g.w.writeIndent("return _f.onConstruct(_f.mem.memoize%s(&%s))\n", define.name, define.varName)
		g.w.unnest(1, "}\n\n")
	}

	g.genDynamicConstructLookup()
}

func (g *FactoryGen) genRule(rule *xformRule) {
	g.w.writeIndent("// [%s]\n", rule.name)
	g.w.nest("{\n")

	// Do initial pass over rule match parse tree, and generate all
	// variable declarations. These need to be done at the top level
	// so that they're accessible to the generated replace code.
	g.genVarDefs(rule, true)

	for index, matchField := range rule.match.Fields() {
		fieldName := g.lookupFieldName(rule.match, index)
		g.genMatch(matchField, fieldName, false)
	}

	g.w.writeIndent("_f.maxSteps--\n")
	g.w.writeIndent("_group = ")
	g.genReplace(rule, rule.replace)
	g.w.write("\n")
	g.w.writeIndent("_f.mem.addAltFingerprint(_fingerprint, _group)\n")
	g.w.writeIndent("return _group\n")

	g.w.unnest(g.w.nesting-1, "}\n")
	g.w.writeIndent("\n")
}

func (g *FactoryGen) genMatch(match Expr, fieldName string, negate bool) {
	if matchFields, ok := match.(*MatchFieldsExpr); ok {
		g.genMatchField(matchFields, fieldName, negate)
		return
	}

	if matchInvoke, ok := match.(*MatchInvokeExpr); ok {
		g.genMatchInvoke(matchInvoke, negate)
		return
	}

	if matchAnd, ok := match.(*MatchAndExpr); ok {
		if negate {
			panic("negate is not yet supported by the and match op")
		}

		g.genMatch(matchAnd.Left(), fieldName, negate)
		g.genMatch(matchAnd.Right(), fieldName, negate)
		return
	}

	if not, ok := match.(*MatchNotExpr); ok {
		g.genMatch(not.Input(), fieldName, !negate)
		return
	}

	if bind, ok := match.(*BindExpr); ok {
		if bind.Label() != fieldName {
			g.w.writeIndent("%s = %s\n", bind.Label(), fieldName)
		}

		g.genMatch(bind.Target(), fieldName, negate)
		return
	}

	if str, ok := match.(*StringExpr); ok {
		if negate {
			g.w.nest("if %s != m.mem.internPrivate(\"%s\") {\n", fieldName, str.Value())
		} else {
			g.w.nest("if %s == m.mem.internPrivate(\"%s\") {\n", fieldName, str.Value())
		}
		return
	}

	if _, ok := match.(*MatchAnyExpr); ok {
		if negate {
			g.w.nest("if false {\n")
		}
		return
	}

	if matchList, ok := match.(*MatchListExpr); ok {
		g.w.nest("for _, _item := range _f.mem.lookupList(%s) {\n", fieldName)
		g.genMatch(matchList.MatchItem(), "_item", negate)
		return
	}

	panic(fmt.Sprintf("unrecognized match expression: %v", match))
}

func (g *FactoryGen) genMatchField(matchFields *MatchFieldsExpr, fieldName string, negate bool) {
	opName := matchFields.OpName()
	numFields := len(matchFields.Fields())
	varName := g.makeUnique(fmt.Sprintf("_%s", unTitle(opName)))

	if negate && numFields != 0 {
		g.w.writeIndent("_match := false\n")
	}

	nesting := g.w.nesting

	g.w.writeIndent("%s := _f.mem.lookupNormExpr(%s).as%s()\n", varName, fieldName, opName)

	if negate && numFields == 0 {
		g.w.nest("if %s == nil {\n", varName)
	} else {
		g.w.nest("if %s != nil {\n", varName)
	}

	for index, matchField := range matchFields.Fields() {
		fieldName := g.lookupFieldName(matchFields, index)
		g.genMatch(matchField, fmt.Sprintf("%s.%s", varName, fieldName), false)
	}

	if negate && numFields != 0 {
		g.w.writeIndent("_match = true\n")
		g.w.unnest(g.w.nesting-nesting, "}\n")
		g.w.writeIndent("\n")
		g.w.nest("if !_match {\n")
	}
}

func (g *FactoryGen) genMatchInvoke(matchInvoke *MatchInvokeExpr, negate bool) {
	funcName := unTitle(matchInvoke.FuncName())

	if negate {
		g.w.nest("if !_f.%s(", funcName)
	} else {
		g.w.nest("if _f.%s(", funcName)
	}

	for index, matchArg := range matchInvoke.Args() {
		ref := matchArg.(*RefExpr)

		if index != 0 {
			g.w.write(", ")
		}

		g.w.write(ref.Label())
	}

	g.w.write(") {\n")
}

func (g *FactoryGen) genReplace(rule *xformRule, replace Expr) {
	if construct, ok := replace.(*ConstructExpr); ok {
		if strName, ok := construct.OpName().(*StringExpr); ok {
			name := strName.ValueAsString()
			define := g.compiled.LookupDefine(name)
			if define != nil {
				// Standard op construction function.
				g.w.write("_f.Construct%s(", name)
			} else {
				// Custom function.
				g.w.write("_f.%s(", unTitle(name))
			}
		}

		if opName, ok := construct.OpName().(*OpNameExpr); ok {
			name := opName.ValueAsOpName()
			g.w.write("_f.Construct%s(", name[:len(name)-2])
		}

		for index, elem := range construct.Args() {
			if index != 0 {
				g.w.write(", ")
			}

			g.genReplace(rule, elem)
		}

		g.w.write(")")
		return
	}

	if constructList, ok := replace.(*ConstructListExpr); ok {
		g.w.write("_f.mem.storeList([]GroupID{")

		for index, elem := range constructList.Children() {
			if index != 0 {
				g.w.write(", ")
			}

			g.genReplace(rule, elem)
		}

		g.w.write("})")
		return
	}

	if ref, ok := replace.(*RefExpr); ok {
		g.w.write(ref.Label())
		return
	}

	if str, ok := replace.(*StringExpr); ok {
		g.w.write("m.mem.internPrivate(\"%s\")", str.Value())
		return
	}

	if opName, ok := replace.(*OpNameExpr); ok {
		g.w.write(opName.ValueAsOpName())
		return
	}

	panic(fmt.Sprintf("unrecognized replace expression: %v", replace))
}

func (g *FactoryGen) genDynamicConstructLookup() {
	// Generate dynamic construct lookup table.
	g.w.writeIndent("type dynConstructLookupFunc func(f *Factory, children []GroupID, private PrivateID) GroupID\n")

	g.w.nest("var dynConstructLookup = []dynConstructLookupFunc{\n")
	g.w.writeIndent("// UnknownOp\n")
	g.w.writeIndent("func(f *Factory, children []GroupID, private PrivateID) GroupID {\n")
	g.w.writeIndent("  panic(\"op type not initialized\")\n")
	g.w.writeIndent("},\n\n")

	for _, define := range g.defines {
		g.w.writeIndent("// %sOp\n", define.name)
		g.w.nest("func(f *Factory, children []GroupID, private PrivateID) GroupID {\n")

		g.w.writeIndent("return f.Construct%s(", define.name)
		for i, field := range define.fields {
			if i != 0 {
				g.w.write(", ")
			}

			if field.isList() {
				if i == 0 {
					g.w.write("f.StoreList(children)")
				} else {
					g.w.write("f.StoreList(children[%d:])", i)
				}
			} else if field.isPrivate() {
				g.w.write("private")
			} else {
				g.w.write("children[%d]", i)
			}
		}
		g.w.write(")\n")

		g.w.unnest(1, "},\n\n")
	}

	g.w.unnest(1, "}\n\n")

	g.w.nest("func (f *Factory) dynamicConstruct(op Operator, children []GroupID, private PrivateID) GroupID {\n")
	g.w.writeIndent("return dynConstructLookup[op](f, children, private)\n")
	g.w.unnest(1, "}\n")
}
