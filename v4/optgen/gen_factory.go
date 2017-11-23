package main

import (
	"fmt"
	"io"
)

func (g *generator) generateFactory(w io.Writer) {
	fmt.Fprintf(w, "package %s\n\n", g.pkg)

	for _, elem := range g.root.Defines().All() {
		define := elem.AsDefine()

		fmt.Fprintf(w, "func (f *factory) construct%s(\n", define.Name())

		for _, elem := range define.Fields() {
			field := elem.AsDefineField()
			fmt.Fprintf(w, "  %s %s,\n", unTitle(field.Name()), mapType(field.Type()))
		}

		fmt.Fprintf(w, ") exprOffset {\n")

		hasRule := false
		for _, elem := range g.root.Rules().All() {
			rule := elem.AsRule()
			if rule.Match().Op() != define.Name() {
				continue
			}

			fmt.Fprintf(w, "  // [%s]\n", rule.Header().Name())
			fmt.Fprintf(w, "  for {\n")

			g.unique = make(map[string]bool)

			for index, matchField := range rule.Match().Fields() {
				defineField := g.lookupField(rule.Match().Op(), index)
				if defineField == nil {
					panic(fmt.Sprintf("unrecognized pattern match operation '%s'", rule.Match().Op()))
				}

				g.generateMatch(w, matchField, defineField.Name())
			}

			fmt.Fprintf(w, "\n")
			fmt.Fprintf(w, "    return ")
			g.generateReplace(w, rule.Replace())
			fmt.Fprintf(w, "\n")

			fmt.Fprintf(w, "  }\n")

			hasRule = true
		}

		if hasRule {
			fmt.Fprintf(w, "\n")
		}

		exprName := fmt.Sprintf("%sExpr", unTitle(define.Name()))
		fmt.Fprintf(w, "  %s := &%s{op: %sOp", exprName, exprName, unTitle(define.Name()))

		for _, elem := range define.Fields() {
			field := elem.AsDefineField()
			fieldName := unTitle(field.Name())
			fmt.Fprintf(w, ", %s: %s", fieldName, fieldName)
		}

		fmt.Fprintf(w, "}\n")
		fmt.Fprintf(w, "  return f.memo.memoize%sExpr(%s)\n", define.Name(), exprName)
		fmt.Fprintf(w, "}\n\n")
	}
}

func (g *generator) generateMatch(w io.Writer, match *Expr, fieldName string) {
	if matchList := match.AsMatchList(); matchList != nil {
		for _, matchField := range matchList.All() {
			g.generateMatch(w, matchField, fieldName)
		}

		return
	}

	if matchFields := match.AsMatchFields(); matchFields != nil {
		op := matchFields.Op()
		define := g.lookupOp(op)
		if define != nil {
			exprName := fmt.Sprintf("%sExpr", g.uniquify(unTitle(op)))
			fmt.Fprintf(w, "    %s := f.memo.lookupExpr(%s).as%s()\n", exprName, unTitle(fieldName), op)
			fmt.Fprintf(w, "    if %s == nil {\n", exprName)
			fmt.Fprintf(w, "      break\n")
			fmt.Fprintf(w, "    }\n\n")

			for index, matchField := range matchFields.Fields() {
				defineField := g.lookupField(op, index)
				g.generateMatch(w, matchField, fmt.Sprintf("%s.%s", exprName, defineField.Name()))
			}
		} else {
			fmt.Fprintf(w, "    if !f.%s(", unTitle(op))

			for _, matchField := range matchFields.Fields() {
				ref := matchField.AsRef()
				if ref == nil {
					panic("user function arguments can only be variable references")
				}

				fmt.Fprint(w, ref.Label())
			}

			fmt.Fprintf(w, ") {\n")

			fmt.Fprintf(w, "      break\n")
			fmt.Fprintf(w, "    }\n\n")
		}

		return
	}

	if str := match.AsString(); str != nil {
		fmt.Fprintf(w, "    if %s != m.memo.internPrivate(\"%s\") {\n", unTitle(fieldName), str.Value())
		fmt.Fprintf(w, "      break\n")
		fmt.Fprintf(w, "    }\n\n")
		return
	}

	if bind := match.AsBind(); bind != nil {
		fmt.Fprintf(w, "    %s := %s\n", bind.Label(), unTitle(fieldName))
		g.generateMatch(w, bind.Target(), fieldName)
		return
	}

	if match.AsMatchAny() != nil {
		return
	}

	panic(fmt.Sprintf("unrecognized match expression: %v", match))
}

func (g *generator) generateReplace(w io.Writer, replace *Expr) {
	if construct := replace.AsConstruct(); construct != nil {
		fmt.Fprintf(w, "f.construct%s(", construct.Op())

		for index, elem := range construct.All() {
			if index != 0 {
				fmt.Fprintf(w, ", ")
			}

			g.generateReplace(w, elem)
		}

		fmt.Fprintf(w, ")")
		return
	}

	if ref := replace.AsRef(); ref != nil {
		fmt.Fprint(w, ref.Label())
		return
	}

	if str := replace.AsString(); str != nil {
		fmt.Fprintf(w, "m.memo.internPrivate(\"%s\")", str.Value())
		return
	}

	panic(fmt.Sprintf("unrecognized replace expression: %v", replace))
}
