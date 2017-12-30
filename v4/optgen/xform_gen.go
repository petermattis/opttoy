package main

import (
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"
)

type xformGen struct {
	compiled CompiledExpr
	w        matchWriter
	rules    []*xformRule
	defines  []*xformDefine
	unique   map[string]bool
}

type xformDefine struct {
	name     string
	exprType string
	opType   string
	varName  string
	tags     tagList
	fields   []*xformDefineField
	list     *xformDefineField
	private  *xformDefineField
	rules    []*xformRule
}

type xformDefineField struct {
	define *xformDefine
	name   string
	typ    string
}

type xformRule struct {
	define  *xformDefine
	name    string
	tags    tagList
	match   *MatchFieldsExpr
	replace ParsedExpr
}

func (x *xformGen) init(compiled CompiledExpr, w io.Writer, ruleType string) {
	x.compiled = compiled
	x.w = matchWriter{writer: w}
	x.rules = x.createRules(ruleType)
	x.defines = x.createDefines()
	x.unique = make(map[string]bool)
}

func (x *xformGen) genVarDefs(rule *xformRule, excludeTopLevel bool) {
	var traverse func(match ParsedExpr, fieldType, excludeFieldName string)

	hasVarDef := false
	traverse = func(match ParsedExpr, fieldType, excludeFieldName string) {
		if matchFields, ok := match.(*MatchFieldsExpr); ok {
			for index, matchField := range matchFields.Fields() {
				fieldDef := x.lookupFieldDef(matchFields, index)
				traverse(matchField, fieldDef.Type(), "")
			}
			return
		}

		if _, ok := match.(*MatchListExpr); ok {
			// MatchList matches items in a list rather than fields in a struct.
			fieldType = "Expr"
			excludeFieldName = ""
		}

		if bind, ok := match.(*BindExpr); ok {
			if bind.Label() != excludeFieldName {
				x.w.writeIndent("var %s %s\n", bind.Label(), mapType(fieldType))
				hasVarDef = true
			}
		}

		for _, child := range match.Children() {
			traverse(child, fieldType, excludeFieldName)
		}
	}

	for index, matchField := range rule.match.Fields() {
		fieldDef := x.lookupFieldDef(rule.match, index)
		if excludeTopLevel {
			traverse(matchField, fieldDef.Type(), unTitle(fieldDef.Name()))
		} else {
			traverse(matchField, fieldDef.Type(), "")
		}
	}

	if hasVarDef {
		x.w.write("\n")
	}
}

func (x *xformGen) makeUnique(s string) string {
	try := s
	for i := 2; ; i++ {
		_, ok := x.unique[try]
		if !ok {
			x.unique[try] = true
			return try
		}

		try = fmt.Sprintf("%s%d", s, i)
	}
}

func (x *xformGen) lookupFieldDef(matchFields *MatchFieldsExpr, index int) *DefineFieldExpr {
	define := x.compiled.LookupDefine(matchFields.OpName())
	return define.Fields()[index].(*DefineFieldExpr)
}

func (x *xformGen) lookupFieldName(matchFields *MatchFieldsExpr, index int) string {
	return unTitle(x.lookupFieldDef(matchFields, index).Name())
}

func (x *xformGen) createRules(ruleType string) []*xformRule {
	var xrulesList []*xformRule

	for _, rule := range x.compiled.Rules() {
		var xrule xformRule

		// Only add rules of the specified type.
		if !rule.Header().Tags().Contains(ruleType) {
			continue
		}

		for _, elem := range rule.Header().Tags().All() {
			xrule.tags = append(xrule.tags, elem.(*StringExpr).ValueAsString())
		}

		xrule.name = rule.Header().Name()
		xrule.match = rule.Match().(*MatchFieldsExpr)
		xrule.replace = rule.Replace()
		xrulesList = append(xrulesList, &xrule)
	}

	return xrulesList
}

func (x *xformGen) createDefines() []*xformDefine {
	var xdefineList []*xformDefine

	for _, define := range x.compiled.Defines() {
		var xdefine xformDefine

		if define.HasTag("Enforcer") {
			// Don't create transform methods for enforcers, since they're only
			// created by the optimizer.
			continue
		}

		xdefine.name = define.Name()
		xdefine.exprType = fmt.Sprintf("%sExpr", unTitle(define.Name()))
		xdefine.opType = fmt.Sprintf("%sOp", define.Name())
		xdefine.varName = fmt.Sprintf("_%s", xdefine.exprType)

		// Create list of tags that are associated with the define.
		for _, elem := range define.Tags().All() {
			xdefine.tags = append(xdefine.tags, elem.(*StringExpr).ValueAsString())
		}

		var xfieldList []*xformDefineField
		for _, elem := range define.Fields() {
			field := elem.(*DefineFieldExpr)
			xfield := &xformDefineField{define: &xdefine, name: unTitle(field.Name()), typ: mapType(field.Type())}
			xfieldList = append(xfieldList, xfield)

			if field.IsListType() {
				xdefine.list = xfield
			} else if field.IsPrivateType() {
				xdefine.private = xfield
			}
		}
		xdefine.fields = xfieldList

		// Add all rules to the define that have a root match fields expression
		// with a matching opname.
		var xrulesList []*xformRule
		for _, rule := range x.rules {
			if rule.match.OpName() == define.Name() {
				xrulesList = append(xrulesList, rule)
				rule.define = &xdefine
			}
		}
		xdefine.rules = xrulesList

		xdefineList = append(xdefineList, &xdefine)
	}

	return xdefineList
}

func (x *xformDefineField) isList() bool {
	return x.define.list == x
}

func (x *xformDefineField) isPrivate() bool {
	return x.define.private == x
}

type tagList []string

func (t tagList) contains(tag string) bool {
	for _, t := range t {
		if t == tag {
			return true
		}
	}
	return false
}

func unTitle(name string) string {
	rune, size := utf8.DecodeRuneInString(name)
	return fmt.Sprintf("%c%s", unicode.ToLower(rune), name[size:])
}

func mapType(typ string) string {
	switch typ {
	case "Expr":
		return "GroupID"

	case "ExprList":
		return "ListID"

	default:
		return "PrivateID"
	}
}
