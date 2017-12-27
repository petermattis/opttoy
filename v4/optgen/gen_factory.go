package main

import (
	"fmt"
	"io"
)

func (g *generator) genFactory(_w io.Writer) {
	w := &matchWriter{writer: _w}

	w.writeIndent("package %s\n\n", g.pkg)

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)

		if define.HasTag("Enforcer") {
			// Don't create factory methods for enforcers, since they're only
			// created by the optimizer.
			continue
		}

		exprType := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		opType := fmt.Sprintf("%sOp", define.Name())
		varName := fmt.Sprintf("_%s", exprType)

		w.writeIndent("func (_f *Factory) Construct%s(\n", define.Name())

		for _, elem := range define.Fields() {
			field := elem.(*DefineFieldExpr)
			w.writeIndent("  %s %s,\n", unTitle(field.Name()), mapType(field.Type()))
		}

		w.nest(") GroupID {\n")

		w.writeIndent("%s := %s{memoExpr: memoExpr{op: %s}", varName, exprType, opType)

		for _, elem := range define.Fields() {
			field := elem.(*DefineFieldExpr)
			fieldName := unTitle(field.Name())
			w.write(", %s: %s", fieldName, fieldName)
		}

		w.write("}\n")
		w.writeIndent("_fingerprint := %s.fingerprint()\n", varName)
		w.writeIndent("_group := _f.mem.lookupGroupByFingerprint(_fingerprint)\n")
		w.nest("if _group != 0 {\n")
		w.writeIndent("return _group\n")
		w.unnest(1)
		w.write("\n")

		hasRule := false
		for _, elem := range g.compiled.Root().Rules().All() {
			rule := elem.(*RuleExpr)
			if rule.Match().OpName() != define.Name() {
				continue
			}

			if !rule.Header().Tags().Contains("Normalize") {
				continue
			}

			g.unique = make(map[string]bool)

			w.writeIndent("// [%s]\n", rule.Header().Name())
			w.nest("{\n")

			// Do initial pass over rule match parse tree, and generate all
			// variable declarations. These need to be done at the top level
			// so that they're accessible to the generated replace code.
			hasVarDef := false
			for index, matchField := range rule.Match().Fields() {
				fieldName := g.lookupFieldName(rule.Match(), index)
				hasVarDef = hasVarDef || g.genFactoryVarDefs(w, matchField, fieldName)
			}

			if hasVarDef {
				w.write("\n")
			}

			for index, matchField := range rule.Match().Fields() {
				fieldName := g.lookupFieldName(rule.Match(), index)
				g.genFactoryMatch(w, matchField, fieldName, false)
			}

			w.writeIndent("_group = ")
			g.genFactoryReplace(w, rule.Replace())
			w.write("\n")
			w.writeIndent("_f.mem.addAltFingerprint(_fingerprint, _group)\n")
			w.writeIndent("return _group\n")

			w.unnest(w.nesting - 1)
			w.write("\n")

			hasRule = true
		}

		if hasRule {
			w.write("\n")
		}

		w.writeIndent("return _f.mem.memoize%s(&%s)\n", define.Name(), varName)
		w.unnest(1)
		w.write("\n")
	}
}

func (g *generator) genFactoryVarDefs(w *matchWriter, match ParsedExpr, fieldName string) bool {
	if _, ok := match.(*MatchFieldsExpr); ok {
		fieldName = ""
	}

	if bind, ok := match.(*BindExpr); ok {
		if bind.Label() != fieldName {
			w.writeIndent("var %s GroupID\n", bind.Label())
			return true
		}
	}

	hasVarDef := false
	for _, child := range match.Children() {
		hasVarDef = hasVarDef || g.genFactoryVarDefs(w, child, fieldName)
	}

	return hasVarDef
}

func (g *generator) genFactoryMatch(w *matchWriter, match ParsedExpr, fieldName string, negate bool) {
	if matchFields, ok := match.(*MatchFieldsExpr); ok {
		opName := matchFields.OpName()
		numFields := len(matchFields.Fields())
		varName := g.makeUnique(fmt.Sprintf("_%s", unTitle(opName)))

		if negate && numFields != 0 {
			w.writeIndent("match := false\n")
		}

		nesting := w.nesting

		w.writeIndent("%s := _f.mem.lookupNormExpr(%s).as%s()\n", varName, fieldName, opName)

		if negate && numFields == 0 {
			w.nest("if %s == nil {\n", varName)
		} else {
			w.nest("if %s != nil {\n", varName)
		}

		for index, matchField := range matchFields.Fields() {
			fieldName := g.lookupFieldName(matchFields, index)
			g.genFactoryMatch(w, matchField, fmt.Sprintf("%s.%s", varName, fieldName), false)
		}

		if negate && numFields != 0 {
			w.writeIndent("match = true\n")
			w.unnest(w.nesting - nesting)
			w.writeIndent("\n")
			w.nest("if !match {\n")
		}

		return
	}

	if matchInvoke, ok := match.(*MatchInvokeExpr); ok {
		funcName := unTitle(matchInvoke.FuncName())

		if negate {
			w.nest("if !_f.%s(", funcName)
		} else {
			w.nest("if _f.%s(", funcName)
		}

		for index, matchArg := range matchInvoke.Args() {
			ref := matchArg.(*RefExpr)

			if index != 0 {
				w.write(", ")
			}

			w.write(ref.Label())
		}

		w.write(") {\n")
		return
	}

	if matchAnd, ok := match.(*MatchAndExpr); ok {
		if negate {
			panic("negate is not yet supported by the and match op")
		}

		g.genFactoryMatch(w, matchAnd.Left(), fieldName, negate)
		g.genFactoryMatch(w, matchAnd.Right(), fieldName, negate)
		return
	}

	if not, ok := match.(*MatchNotExpr); ok {
		g.genFactoryMatch(w, not.Input(), fieldName, !negate)
		return
	}

	if bind, ok := match.(*BindExpr); ok {
		if bind.Label() != fieldName {
			w.writeIndent("%s = %s\n", bind.Label(), fieldName)
		}

		g.genFactoryMatch(w, bind.Target(), fieldName, negate)
		return
	}

	if str, ok := match.(*StringExpr); ok {
		if negate {
			w.nest("if %s != m.mem.internPrivate(\"%s\") {\n", fieldName, str.Value())
		} else {
			w.nest("if %s == m.mem.internPrivate(\"%s\") {\n", fieldName, str.Value())
		}

		return
	}

	if _, ok := match.(*MatchAnyExpr); ok {
		if negate {
			w.nest("if false {\n")
		}

		return
	}

	panic(fmt.Sprintf("unrecognized match expression: %v", match))
}

func (g *generator) genFactoryReplace(w *matchWriter, replace ParsedExpr) {
	if construct, ok := replace.(*ConstructExpr); ok {
		w.write("_f.Construct%s(", construct.Name())

		for index, elem := range construct.All() {
			if index != 0 {
				w.write(", ")
			}

			g.genFactoryReplace(w, elem)
		}

		w.write(")")
		return
	}

	if ref, ok := replace.(*RefExpr); ok {
		w.write(ref.Label())
		return
	}

	if str, ok := replace.(*StringExpr); ok {
		w.write("m.mem.internPrivate(\"%s\")", str.Value())
		return
	}

	panic(fmt.Sprintf("unrecognized replace expression: %v", replace))
}
