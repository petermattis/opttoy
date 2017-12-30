package main

import (
	"bytes"
	"fmt"
	"strings"
)

type ParsedExpr interface {
	Op() operator
	Children() []ParsedExpr
	ChildName(pos int) string
	Value() interface{}

	String() string
	Format(buf *bytes.Buffer, level int)
}

type expr struct {
	op       operator
	children []ParsedExpr
	names    map[int]string
	value    interface{}
}

func (e *expr) Op() operator {
	return e.op
}

func (e *expr) Children() []ParsedExpr {
	return e.children
}

func (e *expr) ChildName(pos int) string {
	return e.names[pos]
}

func (e *expr) Value() interface{} {
	return e.value
}

func (e *expr) String() string {
	var buf bytes.Buffer
	e.Format(&buf, 0)
	return buf.String()
}

func (e *expr) Format(buf *bytes.Buffer, level int) {
	if e.value != nil {
		if s, ok := e.value.(string); ok {
			buf.WriteByte('"')
			buf.WriteString(s)
			buf.WriteByte('"')
		} else {
			buf.WriteString(fmt.Sprintf("%v", e.value))
		}

		return
	}

	opName := strings.Title(e.op.String())
	opName = opName[:len(opName)-2]

	if len(e.children) == 0 {
		buf.WriteByte('(')
		buf.WriteString(opName)
		buf.WriteByte(')')
		return
	}

	nested := false
	for _, child := range e.children {
		if child.Value() == nil && len(child.Children()) != 0 {
			nested = true
			break
		}
	}

	if !nested {
		buf.WriteByte('(')
		buf.WriteString(opName)

		for i, child := range e.children {
			buf.WriteByte(' ')

			if i < len(e.names) {
				buf.WriteString(e.names[i])
				buf.WriteByte('=')
			}

			child.Format(buf, level)
		}

		buf.WriteByte(')')
	} else {
		buf.WriteByte('(')
		buf.WriteString(opName)
		buf.WriteByte('\n')
		level++

		for i, child := range e.children {
			writeIndent(buf, level)

			if i < len(e.names) {
				buf.WriteString(e.names[i])
				buf.WriteByte('=')
			}

			child.Format(buf, level)
			buf.WriteByte('\n')
		}

		level--
		writeIndent(buf, level)
		buf.WriteByte(')')
	}
}

type RootExpr struct {
	expr
}

func NewRootExpr() *RootExpr {
	children := []ParsedExpr{
		NewDefineSetExpr(),
		NewRuleSetExpr(),
	}

	names := map[int]string{0: "Defines", 1: "Rules"}

	return &RootExpr{expr{op: rootOp, children: children, names: names}}
}

func (e *RootExpr) Defines() *DefineSetExpr {
	return e.children[0].(*DefineSetExpr)
}

func (e *RootExpr) Rules() *RuleSetExpr {
	return e.children[1].(*RuleSetExpr)
}

type DefineSetExpr struct {
	expr
}

func NewDefineSetExpr() *DefineSetExpr {
	return &DefineSetExpr{expr{op: defineSetOp}}
}

func (e *DefineSetExpr) All() []ParsedExpr {
	return e.children
}

func (e *DefineSetExpr) Add(define *DefineExpr) {
	e.children = append(e.children, define)
}

type DefineExpr struct {
	expr
}

func NewDefineExpr(name string, tags []string) *DefineExpr {
	children := []ParsedExpr{
		NewStringExpr(name),
		NewTagsExpr(tags),
	}

	names := map[int]string{0: "Name", 1: "Tags"}

	return &DefineExpr{expr{op: defineOp, children: children, names: names}}
}

func (e *DefineExpr) Name() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *DefineExpr) Tags() *TagsExpr {
	return e.children[1].(*TagsExpr)
}

func (e *DefineExpr) ListField() *DefineFieldExpr {
	// If list-typed field is present, it will be the last field, or the second
	// to last field if a private field is present.
	index := len(e.children) - 1
	if e.PrivateField() != nil {
		index--
	}

	if index < 2 {
		return nil
	}

	defineField := e.children[index].(*DefineFieldExpr)
	if defineField.IsListType() {
		return defineField
	}

	return nil
}

func (e *DefineExpr) PrivateField() *DefineFieldExpr {
	// If private is present, it will be the last field.
	index := len(e.children) - 1
	if index < 2 {
		return nil
	}

	defineField := e.children[index].(*DefineFieldExpr)
	if defineField.IsPrivateType() {
		return defineField
	}

	return nil
}

func (e *DefineExpr) Fields() []ParsedExpr {
	return e.children[2:]
}

func (e *DefineExpr) Add(field *DefineFieldExpr) {
	e.children = append(e.children, field)
}

func (e *DefineExpr) HasTag(tag string) bool {
	for _, elem := range e.Tags().All() {
		s := elem.(*StringExpr)
		if s.ValueAsString() == tag {
			return true
		}
	}

	return false
}

type DefineFieldExpr struct {
	expr
}

func NewDefineFieldExpr(name, typ string) *DefineFieldExpr {
	children := []ParsedExpr{
		NewStringExpr(name),
		NewStringExpr(typ),
	}

	names := map[int]string{0: "Name", 1: "Type"}

	return &DefineFieldExpr{expr{op: defineFieldOp, children: children, names: names}}
}

func (e *DefineFieldExpr) Name() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *DefineFieldExpr) Type() string {
	return e.children[1].(*StringExpr).ValueAsString()
}

func (e *DefineFieldExpr) IsExprType() bool {
	return e.Type() == "Expr"
}

func (e *DefineFieldExpr) IsListType() bool {
	return e.Type() == "ExprList"
}

func (e *DefineFieldExpr) IsPrivateType() bool {
	typ := e.Type()
	return typ != "Expr" && typ != "ExprList"
}

type RuleSetExpr struct {
	expr
}

func NewRuleSetExpr() *RuleSetExpr {
	return &RuleSetExpr{expr{op: ruleSetOp}}
}

func (e *RuleSetExpr) All() []ParsedExpr {
	return e.children
}

func (e *RuleSetExpr) Add(rule *RuleExpr) {
	e.children = append(e.children, rule)
}

type RuleExpr struct {
	expr
}

func NewRuleExpr(header *RuleHeaderExpr, match ParsedExpr, replace ParsedExpr) *RuleExpr {
	children := []ParsedExpr{
		header,
		match,
		replace,
	}

	names := map[int]string{0: "Header", 1: "Match", 2: "Replace"}

	return &RuleExpr{expr{op: ruleOp, children: children, names: names}}
}

func (e *RuleExpr) Header() *RuleHeaderExpr {
	return e.children[0].(*RuleHeaderExpr)
}

func (e *RuleExpr) Match() ParsedExpr {
	return e.children[1]
}

func (e *RuleExpr) Replace() ParsedExpr {
	return e.children[2]
}

type RuleHeaderExpr struct {
	expr
}

func NewRuleHeaderExpr(name string, tags []string) *RuleHeaderExpr {
	children := []ParsedExpr{
		NewStringExpr(name),
		NewTagsExpr(tags),
	}

	names := map[int]string{0: "Name", 1: "Tags"}

	return &RuleHeaderExpr{expr{op: ruleHeaderOp, children: children, names: names}}
}

func (e *RuleHeaderExpr) Name() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *RuleHeaderExpr) Tags() *TagsExpr {
	return e.children[1].(*TagsExpr)
}

type BindExpr struct {
	expr
}

func NewBindExpr(label string, target ParsedExpr) *BindExpr {
	children := []ParsedExpr{
		NewStringExpr(label),
		target,
	}

	names := map[int]string{0: "Label", 1: "Target"}

	return &BindExpr{expr{op: bindOp, children: children, names: names}}
}

func (e *BindExpr) Label() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *BindExpr) Target() ParsedExpr {
	return e.children[1]
}

type RefExpr struct {
	expr
}

func NewRefExpr(label string) *RefExpr {
	children := []ParsedExpr{
		NewStringExpr(label),
	}

	names := map[int]string{0: "Label"}

	return &RefExpr{expr{op: refOp, children: children, names: names}}
}

func (e *RefExpr) Label() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

type MatchTemplateExpr struct {
	expr
}

func NewMatchTemplateExpr(opNames *MatchTemplateNamesExpr) *MatchTemplateExpr {
	children := []ParsedExpr{
		opNames,
	}

	names := map[int]string{0: "Names"}

	return &MatchTemplateExpr{expr{op: matchTemplateOp, children: children, names: names}}
}

func (e *MatchTemplateExpr) Names() *MatchTemplateNamesExpr {
	return e.children[0].(*MatchTemplateNamesExpr)
}

func (e *MatchTemplateExpr) Fields() []ParsedExpr {
	return e.children[1:]
}

func (e *MatchTemplateExpr) Add(match ParsedExpr) {
	e.children = append(e.children, match)
}

type MatchTemplateNamesExpr struct {
	expr
}

func NewMatchTemplateNamesExpr() *MatchTemplateNamesExpr {
	return &MatchTemplateNamesExpr{expr{op: matchTemplateNamesOp}}
}

func (e *MatchTemplateNamesExpr) Add(name *StringExpr) {
	e.children = append(e.children, name)
}

type MatchAndExpr struct {
	expr
}

func NewMatchAndExpr(left, right ParsedExpr) *MatchAndExpr {
	return &MatchAndExpr{expr{op: matchAndOp, children: []ParsedExpr{left, right}}}
}

func (e *MatchAndExpr) Left() ParsedExpr {
	return e.children[0]
}

func (e *MatchAndExpr) Right() ParsedExpr {
	return e.children[1]
}

type MatchInvokeExpr struct {
	expr
}

func NewMatchInvokeExpr(funcName string) *MatchInvokeExpr {
	children := []ParsedExpr{
		NewStringExpr(funcName),
	}

	names := map[int]string{0: "FuncName"}

	return &MatchInvokeExpr{expr{op: matchInvokeOp, children: children, names: names}}
}

func (e *MatchInvokeExpr) FuncName() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *MatchInvokeExpr) Args() []ParsedExpr {
	return e.children[1:]
}

func (e *MatchInvokeExpr) Add(match ParsedExpr) {
	e.children = append(e.children, match)
}

type MatchFieldsExpr struct {
	expr
}

func NewMatchFieldsExpr(opName string) *MatchFieldsExpr {
	children := []ParsedExpr{
		NewStringExpr(opName),
	}

	names := map[int]string{0: "OpName"}

	return &MatchFieldsExpr{expr{op: matchFieldsOp, children: children, names: names}}
}

func (e *MatchFieldsExpr) OpName() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *MatchFieldsExpr) Fields() []ParsedExpr {
	return e.children[1:]
}

func (e *MatchFieldsExpr) Add(match ParsedExpr) {
	e.children = append(e.children, match)
}

type MatchNotExpr struct {
	expr
}

func NewMatchNotExpr(input ParsedExpr) *MatchNotExpr {
	return &MatchNotExpr{expr{op: matchNotOp, children: []ParsedExpr{input}}}
}

func (e *MatchNotExpr) Input() ParsedExpr {
	return e.children[0]
}

type MatchAnyExpr struct {
	expr
}

var matchAnySingleton = &MatchAnyExpr{expr{op: matchAnyOp}}

func NewMatchAnyExpr() *MatchAnyExpr {
	return matchAnySingleton
}

type MatchListExpr struct {
	expr
}

func NewMatchListExpr(matchItem ParsedExpr) *MatchListExpr {
	return &MatchListExpr{expr{op: matchListOp, children: []ParsedExpr{matchItem}}}
}

func (e *MatchListExpr) MatchItem() ParsedExpr {
	return e.children[0]
}

type ReplaceListExpr struct {
	expr
}

func NewReplaceRootExpr() *ReplaceListExpr {
	return &ReplaceListExpr{expr{op: replaceRootOp}}
}

func (e *ReplaceListExpr) All() []ParsedExpr {
	return e.children
}

func (e *ReplaceListExpr) Add(replace ParsedExpr) {
	e.children = append(e.children, replace)
}

type ConstructExpr struct {
	expr
}

func NewConstructExpr(op string) *ConstructExpr {
	children := []ParsedExpr{
		NewStringExpr(op),
	}

	names := map[int]string{0: "Name"}

	return &ConstructExpr{expr{op: constructOp, children: children, names: names}}
}

func (e *ConstructExpr) Name() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *ConstructExpr) All() []ParsedExpr {
	return e.children[1:]
}

func (e *ConstructExpr) Add(replace ParsedExpr) {
	e.children = append(e.children, replace)
}

type ConstructListExpr struct {
	expr
}

func NewConstructListExpr() *ConstructListExpr {
	return &ConstructListExpr{expr{op: constructListOp}}
}

func (e *ConstructListExpr) Add(item ParsedExpr) {
	e.children = append(e.children, item)
}

type TagsExpr struct {
	expr
}

func NewTagsExpr(tags []string) *TagsExpr {
	e := &TagsExpr{expr{op: tagsOp}}

	for _, tag := range tags {
		e.children = append(e.children, NewStringExpr(tag))
	}

	return e
}

func (e *TagsExpr) All() []ParsedExpr {
	return e.children
}

func (e *TagsExpr) Contains(tag string) bool {
	for _, elem := range e.children {
		value := elem.(*StringExpr).Value()
		if value == tag {
			return true
		}
	}

	return false
}

type StringExpr struct {
	expr
}

func NewStringExpr(s string) *StringExpr {
	return &StringExpr{expr{op: stringOp, value: s}}
}

func (e *StringExpr) ValueAsString() string {
	return e.value.(string)
}

func writeIndent(buf *bytes.Buffer, level int) {
	buf.WriteString(strings.Repeat("  ", level))
}
