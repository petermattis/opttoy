package main

import (
	"bytes"
	"fmt"
	"strings"
)

type Expr struct {
	op       operator
	children []*Expr
	names    []string
	private  interface{}
}

func (e *Expr) Op() operator {
	return e.op
}

func (e *Expr) Children() []*Expr {
	return e.children
}

func (e *Expr) AsRoot() *RootExpr {
	if e.op != rootOp {
		return nil
	}

	return (*RootExpr)(e)
}

func (e *Expr) AsDefineList() *DefineListExpr {
	if e.op != defineListOp {
		return nil
	}

	return (*DefineListExpr)(e)
}

func (e *Expr) AsDefine() *DefineExpr {
	if e.op != defineOp {
		return nil
	}

	return (*DefineExpr)(e)
}

func (e *Expr) AsDefineField() *DefineFieldExpr {
	if e.op != defineFieldOp {
		return nil
	}

	return (*DefineFieldExpr)(e)
}

func (e *Expr) AsRuleList() *RuleListExpr {
	if e.op != ruleListOp {
		return nil
	}

	return (*RuleListExpr)(e)
}

func (e *Expr) AsRuleHeader() *RuleHeaderExpr {
	if e.op != ruleHeaderOp {
		return nil
	}

	return (*RuleHeaderExpr)(e)
}

func (e *Expr) AsRule() *RuleExpr {
	if e.op != ruleOp {
		return nil
	}

	return (*RuleExpr)(e)
}

func (e *Expr) AsBind() *BindExpr {
	if e.op != bindOp {
		return nil
	}

	return (*BindExpr)(e)
}

func (e *Expr) AsRef() *RefExpr {
	if e.op != refOp {
		return nil
	}

	return (*RefExpr)(e)
}

func (e *Expr) AsMatchList() *MatchListExpr {
	if e.op != matchListOp {
		return nil
	}

	return (*MatchListExpr)(e)
}

func (e *Expr) AsMatchFields() *MatchFieldsExpr {
	if e.op != matchFieldsOp {
		return nil
	}

	return (*MatchFieldsExpr)(e)
}

func (e *Expr) AsMatchAny() *MatchAnyExpr {
	if e.op != matchAnyOp {
		return nil
	}

	return (*MatchAnyExpr)(e)
}

func (e *Expr) AsReplaceList() *ReplaceListExpr {
	if e.op != replaceListOp {
		return nil
	}

	return (*ReplaceListExpr)(e)
}

func (e *Expr) AsConstruct() *ConstructExpr {
	if e.op != constructOp {
		return nil
	}

	return (*ConstructExpr)(e)
}

func (e *Expr) AsString() *StringExpr {
	if e.op != stringOp {
		return nil
	}

	return (*StringExpr)(e)
}

func (e *Expr) String() string {
	var buf indentBuffer
	e.format(&buf)
	return buf.String()
}

func (e *Expr) format(buf *indentBuffer) {
	if e.private != nil {
		if s, ok := e.private.(string); ok {
			buf.write("\"%s\"", s)
		} else {
			buf.write("%v", e.private)
		}

		return
	}

	opName := strings.Title(e.op.String())
	opName = opName[:len(opName)-2]

	if len(e.children) == 0 {
		buf.write("(%s)", opName)
		return
	}

	nested := false
	for _, child := range e.children {
		if child.private == nil && len(child.children) != 0 {
			nested = true
			break
		}
	}

	if !nested {
		buf.write("(%s", opName)

		for i, child := range e.children {
			if i < len(e.names) {
				buf.write(" %s=", e.names[i])
				child.format(buf)
			} else {
				buf.write(" ")
				child.format(buf)
			}
		}

		buf.write(")")
	} else {
		buf.write("(%s\n", opName)
		buf.increaseIndent()

		for i, child := range e.children {
			if i < len(e.names) {
				buf.writeIndented("%s=", e.names[i])
				child.format(buf)
			} else {
				buf.writeIndented("")
				child.format(buf)
			}

			buf.writeIndented("\n")
		}

		buf.decreaseIndent()
		buf.writeIndented(")")
	}
}

type RootExpr Expr

func NewRootExpr() *RootExpr {
	children := []*Expr{
		(*Expr)(NewDefineListExpr()),
		(*Expr)(NewRuleListExpr()),
	}

	names := []string{"Defines", "Rules"}

	return &RootExpr{op: rootOp, children: children, names: names}
}

func (e *RootExpr) Defines() *DefineListExpr {
	return e.children[0].AsDefineList()
}

func (e *RootExpr) Rules() *RuleListExpr {
	return e.children[1].AsRuleList()
}

func (e *RootExpr) String() string {
	return (*Expr)(e).String()
}

type DefineListExpr Expr

func NewDefineListExpr() *DefineListExpr {
	return &DefineListExpr{op: defineListOp}
}

func (e *DefineListExpr) All() []*Expr {
	return e.children
}

func (e *DefineListExpr) Add(define *DefineExpr) {
	e.children = append(e.children, (*Expr)(define))
}

func (e *DefineListExpr) String() string {
	return (*Expr)(e).String()
}

type DefineExpr Expr

func NewDefineExpr(name string) *DefineExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(name)),
	}

	names := []string{"Name"}

	return &DefineExpr{op: defineOp, children: children, names: names}
}

func (e *DefineExpr) Name() string {
	return e.children[0].AsString().Value()
}

func (e *DefineExpr) List() *DefineFieldExpr {
	// If list-typed field is present, it will be the last field, or the second
	// to last field if a private field is present.
	index := len(e.children) - 1
	if e.Private() != nil {
		index--
	}

	if index < 1 {
		return nil
	}

	defineField := e.children[index].AsDefineField()
	if defineField.IsListType() {
		return defineField
	}

	return nil
}

func (e *DefineExpr) Private() *DefineFieldExpr {
	// If private is present, it will be the last field.
	index := len(e.children) - 1
	if index < 1 {
		return nil
	}

	defineField := e.children[index].AsDefineField()
	if defineField.IsPrivateType() {
		return defineField
	}

	return nil
}

func (e *DefineExpr) Fields() []*Expr {
	return e.children[1:]
}

func (e *DefineExpr) Add(field *DefineFieldExpr) {
	e.children = append(e.children, (*Expr)(field))
}

func (e *DefineExpr) String() string {
	return (*Expr)(e).String()
}

type DefineFieldExpr Expr

func NewDefineFieldExpr(name, typ string) *DefineFieldExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(name)),
		(*Expr)(NewStringExpr(typ)),
	}

	names := []string{"Name", "Type"}

	return &DefineFieldExpr{op: defineFieldOp, children: children, names: names}
}

func (e *DefineFieldExpr) Name() string {
	return e.children[0].AsString().Value()
}

func (e *DefineFieldExpr) Type() string {
	return e.children[1].AsString().Value()
}

func (e *DefineFieldExpr) IsListType() bool {
	return e.Type() == "ExprList"
}

func (e *DefineFieldExpr) IsPrivateType() bool {
	typ := e.Type()
	return typ != "Expr" && typ != "ExprList"
}

func (e *DefineFieldExpr) String() string {
	return (*Expr)(e).String()
}

type RuleListExpr Expr

func NewRuleListExpr() *RuleListExpr {
	return &RuleListExpr{op: ruleListOp}
}

func (e *RuleListExpr) All() []*Expr {
	return e.children
}

func (e *RuleListExpr) Add(rule *RuleExpr) {
	e.children = append(e.children, (*Expr)(rule))
}

func (e *RuleListExpr) String() string {
	return (*Expr)(e).String()
}

type RuleExpr Expr

func NewRuleExpr(header *RuleHeaderExpr, matchFields *MatchFieldsExpr, replace *Expr) *RuleExpr {
	children := []*Expr{
		(*Expr)(header),
		(*Expr)(matchFields),
		replace,
	}

	names := []string{"Header", "Match", "Replace"}

	return &RuleExpr{op: ruleOp, children: children, names: names}
}

func (e *RuleExpr) Header() *RuleHeaderExpr {
	return e.children[0].AsRuleHeader()
}

func (e *RuleExpr) Match() *MatchFieldsExpr {
	return e.children[1].AsMatchFields()
}

func (e *RuleExpr) Replace() *Expr {
	return e.children[2]
}

func (e *RuleExpr) String() string {
	return (*Expr)(e).String()
}

type RuleHeaderExpr Expr

func NewRuleHeaderExpr(name string) *RuleHeaderExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(name)),
	}

	names := []string{"Name"}

	return &RuleHeaderExpr{op: ruleHeaderOp, children: children, names: names}
}

func (e *RuleHeaderExpr) Name() string {
	return e.children[0].AsString().Value()
}

func (e *RuleHeaderExpr) String() string {
	return (*Expr)(e).String()
}

type BindExpr Expr

func NewBindExpr(label string, target *Expr) *BindExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(label)),
		target,
	}

	names := []string{"Label", "Target"}

	return &BindExpr{op: bindOp, children: children, names: names}
}

func (e *BindExpr) Label() string {
	return e.children[0].AsString().Value()
}

func (e *BindExpr) Target() *Expr {
	return e.children[1]
}

func (e *BindExpr) String() string {
	return (*Expr)(e).String()
}

type RefExpr Expr

func NewRefExpr(label string) *RefExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(label)),
	}

	names := []string{"Label"}

	return &RefExpr{op: refOp, children: children, names: names}
}

func (e *RefExpr) Label() string {
	return e.children[0].AsString().Value()
}

func (e *RefExpr) String() string {
	return (*Expr)(e).String()
}

type MatchListExpr Expr

func NewMatchListExpr() *MatchListExpr {
	return &MatchListExpr{op: matchListOp}
}

func (e *MatchListExpr) All() []*Expr {
	return e.children
}

func (e *MatchListExpr) Add(match *Expr) {
	e.children = append(e.children, match)
}

func (e *MatchListExpr) String() string {
	return (*Expr)(e).String()
}

type MatchFieldsExpr Expr

func NewMatchFieldsExpr(op string) *MatchFieldsExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(op)),
	}

	names := []string{"Op"}

	return &MatchFieldsExpr{op: matchFieldsOp, children: children, names: names}
}

func (e *MatchFieldsExpr) Op() string {
	return e.children[0].AsString().Value()
}

func (e *MatchFieldsExpr) Fields() []*Expr {
	return e.children[1:]
}

func (e *MatchFieldsExpr) Add(match *Expr) {
	e.children = append(e.children, match)
}

func (e *MatchFieldsExpr) String() string {
	return (*Expr)(e).String()
}

type MatchAnyExpr Expr

var matchAnySingleton = &MatchAnyExpr{op: matchAnyOp}

func NewMatchAnyExpr() *MatchAnyExpr {
	return matchAnySingleton
}

func (e *MatchAnyExpr) String() string {
	return (*Expr)(e).String()
}

type ReplaceListExpr Expr

func NewReplaceListExpr() *ReplaceListExpr {
	return &ReplaceListExpr{op: replaceListOp}
}

func (e *ReplaceListExpr) All() []*Expr {
	return e.children
}

func (e *ReplaceListExpr) Add(replace *Expr) {
	e.children = append(e.children, replace)
}

func (e *ReplaceListExpr) String() string {
	return (*Expr)(e).String()
}

type ConstructExpr Expr

func NewConstructExpr(op string) *ConstructExpr {
	children := []*Expr{
		(*Expr)(NewStringExpr(op)),
	}

	names := []string{"Op"}

	return &ConstructExpr{op: constructOp, children: children, names: names}
}

func (e *ConstructExpr) Op() string {
	return e.children[0].AsString().Value()
}

func (e *ConstructExpr) All() []*Expr {
	return e.children[1:]
}

func (e *ConstructExpr) Add(replace *Expr) {
	e.children = append(e.children, replace)
}

func (e *ConstructExpr) String() string {
	return (*Expr)(e).String()
}

type StringExpr Expr

func NewStringExpr(s string) *StringExpr {
	return &StringExpr{op: stringOp, private: s}
}

func (e *StringExpr) Value() string {
	return e.private.(string)
}

func (e *StringExpr) String() string {
	return (*Expr)(e).String()
}

type indentBuffer struct {
	buf    bytes.Buffer
	indent int
}

func (b *indentBuffer) increaseIndent() {
	b.indent++
}

func (b *indentBuffer) decreaseIndent() {
	if b.indent <= 0 {
		panic("indent cannot be decreased below zero")
	}

	b.indent--
}

func (b *indentBuffer) write(format string, args ...interface{}) {
	fmt.Fprintf(&b.buf, format, args...)
}

func (b *indentBuffer) writeIndented(format string, args ...interface{}) {
	b.buf.WriteString(strings.Repeat("  ", b.indent))
	fmt.Fprintf(&b.buf, format, args...)
}

func (b *indentBuffer) String() string {
	return b.buf.String()
}
