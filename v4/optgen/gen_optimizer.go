package main

import (
	"fmt"
	"io"
)

func (g *generator) genOptimizer(_w io.Writer) {
	w := &matchWriter{writer: _w}

	w.writeIndent("package %s\n\n", g.pkg)

	for _, elem := range g.compiled.Root().Defines().All() {
		define := elem.(*DefineExpr)

		w.writeIndent("func (_o *optimizer) optimize%s(\n", define.Name())

		for _, elem := range define.Fields() {
			field := elem.(*DefineFieldExpr)
			w.writeIndent("  %s %s,\n", unTitle(field.Name()), mapType(field.Type()))
		}

		w.nest(") groupID {\n")

		hasRule := false
		for _, elem := range g.compiled.Root().Rules().All() {
			rule := elem.(*RuleExpr)
			if rule.Match().OpName() != define.Name() {
				continue
			}

			if !rule.Header().Tags().Contains("Explore") {
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
				hasVarDef = hasVarDef || g.generateVarDefs(w, matchField, fieldName)
			}

			if hasVarDef {
				w.write("\n")
			}

			for index, matchField := range rule.Match().Fields() {
				fieldName := g.lookupFieldName(rule.Match(), index)
				g.generateMatch(w, matchField, fieldName, false)
			}

			w.writeIndent("return ")
			g.generateReplace(w, rule.Replace())
			w.write("\n")

			w.unnest(w.nesting - 1)
			w.write("\n")

			hasRule = true
		}

		if hasRule {
			w.write("\n")
		}

		varName := unTitle(define.Name())
		exprName := fmt.Sprintf("%sExpr", varName)
		w.writeIndent("_%s := &%s{op: %sOp", varName, exprName, varName)

		for _, elem := range define.Fields() {
			field := elem.(*DefineFieldExpr)
			fieldName := unTitle(field.Name())
			w.write(", %s: %s", fieldName, fieldName)
		}

		w.write("}\n")
		w.writeIndent("return _f.memo.memoize%s(_%s)\n", define.Name(), varName)
		w.unnest(1)
		w.write("\n")
	}
}

func (g *generator) generateVarDefs(w *matchWriter, match ParsedExpr, fieldName string) bool {
	if _, ok := match.(*MatchFieldsExpr); ok {
		fieldName = ""
	}

	if bind, ok := match.(*BindExpr); ok {
		if bind.Label() != fieldName {
			w.writeIndent("var %s groupID\n", bind.Label())
			return true
		}
	}

	hasVarDef := false
	for _, child := range match.Children() {
		hasVarDef = hasVarDef || g.generateVarDefs(w, child, fieldName)
	}

	return hasVarDef
}

func (g *generator) generateMatch(w *matchWriter, match ParsedExpr, fieldName string, negate bool) {
	if matchFields, ok := match.(*MatchFieldsExpr); ok {
		opName := matchFields.OpName()
		numFields := len(matchFields.Fields())
		varName := g.makeUnique(fmt.Sprintf("_%s", unTitle(opName)))

		if negate && numFields != 0 {
			w.writeIndent("match := false\n")
		}

		nesting := w.nesting

		w.writeIndent("%s := _f.memo.lookupNormExpr(%s).as%s()\n", varName, fieldName, opName)

		if negate && numFields == 0 {
			w.nest("if %s == nil {\n", varName)
		} else {
			w.nest("if %s != nil {\n", varName)
		}

		for index, matchField := range matchFields.Fields() {
			fieldName := g.lookupFieldName(matchFields, index)
			g.generateMatch(w, matchField, fmt.Sprintf("%s.%s", varName, fieldName), false)
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

		g.generateMatch(w, matchAnd.Left(), fieldName, negate)
		g.generateMatch(w, matchAnd.Right(), fieldName, negate)
		return
	}

	if not, ok := match.(*MatchNotExpr); ok {
		g.generateMatch(w, not.Input(), fieldName, !negate)
		return
	}

	if bind, ok := match.(*BindExpr); ok {
		if bind.Label() != fieldName {
			w.writeIndent("%s = %s\n", bind.Label(), fieldName)
		}

		g.generateMatch(w, bind.Target(), fieldName, negate)
		return
	}

	if str, ok := match.(*StringExpr); ok {
		if negate {
			w.nest("if %s != m.memo.storePrivate(\"%s\") {\n", fieldName, str.Value())
		} else {
			w.nest("if %s == m.memo.storePrivate(\"%s\") {\n", fieldName, str.Value())
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

func (g *generator) generateReplace(w *matchWriter, replace ParsedExpr) {
	if construct, ok := replace.(*ConstructExpr); ok {
		w.write("_f.construct%s(", construct.Name())

		for index, elem := range construct.All() {
			if index != 0 {
				w.write(", ")
			}

			g.generateReplace(w, elem)
		}

		w.write(")")
		return
	}

	if ref, ok := replace.(*RefExpr); ok {
		w.write(ref.Label())
		return
	}

	if str, ok := replace.(*StringExpr); ok {
		w.write("m.memo.storePrivate(\"%s\")", str.Value())
		return
	}

	panic(fmt.Sprintf("unrecognized replace expression: %v", replace))
}
