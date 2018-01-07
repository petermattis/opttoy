package optgen

import (
	"bytes"
	"fmt"
	"strings"
)

type AcceptFunc func(expr Expr) Expr

type Expr interface {
	Op() Operator
	Children() []Expr
	ChildName(pos int) string
	Value() interface{}
	Visit(accept AcceptFunc) Expr

	String() string
	Format(buf *bytes.Buffer, level int)
}

type expr struct {
	op       Operator
	children []Expr
	value    interface{}
	names    map[int]string
}

func (e *expr) Op() Operator {
	return e.op
}

func (e *expr) Children() []Expr {
	return e.children
}

func (e *expr) ChildName(pos int) string {
	return e.names[pos]
}

func (e *expr) Value() interface{} {
	return e.value
}

func (e *expr) visitChildren(accept AcceptFunc) (children []Expr, replaced bool) {
	for i, child := range e.children {
		newChild := child.Visit(accept)
		if child != newChild {
			if children == nil {
				children = make([]Expr, len(e.children))
				copy(children, e.children)
			}
			children[i] = newChild
		}
	}

	if children == nil {
		children = e.children
	} else {
		replaced = true
	}
	return
}

func (e *expr) String() string {
	var buf bytes.Buffer
	e.Format(&buf, 0)
	return buf.String()
}

func (e *expr) Format(buf *bytes.Buffer, level int) {
	if e.value != nil {
		if e.op == StringOp {
			buf.WriteByte('"')
			buf.WriteString(e.value.(string))
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

type RootExpr struct{ expr }

func NewRootExpr() *RootExpr {
	children := []Expr{
		NewDefineSetExpr(),
		NewRuleSetExpr(),
	}

	names := map[int]string{0: "Defines", 1: "Rules"}

	return &RootExpr{expr{op: RootOp, children: children, names: names}}
}

func (e *RootExpr) Defines() *DefineSetExpr {
	return e.children[0].(*DefineSetExpr)
}

func (e *RootExpr) Rules() *RuleSetExpr {
	return e.children[1].(*RuleSetExpr)
}

func (e *RootExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&RootExpr{expr{op: RootOp, children: children, names: e.names}})
	}
	return accept(e)
}

type DefineSetExpr struct{ expr }

func NewDefineSetExpr() *DefineSetExpr {
	return &DefineSetExpr{expr{op: DefineSetOp}}
}

func (e *DefineSetExpr) All() []Expr {
	return e.children
}

func (e *DefineSetExpr) Add(define *DefineExpr) {
	e.children = append(e.children, define)
}

func (e *DefineSetExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&DefineSetExpr{expr{op: DefineSetOp, children: children}})
	}
	return accept(e)
}

type DefineExpr struct{ expr }

func NewDefineExpr(name string, tags []string) *DefineExpr {
	children := []Expr{
		NewStringExpr(name),
		NewTagsExpr(tags),
	}

	names := map[int]string{0: "Name", 1: "Tags"}

	return &DefineExpr{expr{op: DefineOp, children: children, names: names}}
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

func (e *DefineExpr) Fields() []Expr {
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

func (e *DefineExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&DefineExpr{expr{op: DefineOp, children: children, names: e.names}})
	}
	return accept(e)
}

type DefineFieldExpr struct{ expr }

func NewDefineFieldExpr(name, typ string) *DefineFieldExpr {
	children := []Expr{
		NewStringExpr(name),
		NewStringExpr(typ),
	}

	names := map[int]string{0: "Name", 1: "Type"}

	return &DefineFieldExpr{expr{op: DefineFieldOp, children: children, names: names}}
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

func (e *DefineFieldExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&DefineFieldExpr{expr{op: DefineFieldOp, children: children, names: e.names}})
	}
	return accept(e)
}

type RuleSetExpr struct{ expr }

func NewRuleSetExpr() *RuleSetExpr {
	return &RuleSetExpr{expr{op: RuleSetOp}}
}

func (e *RuleSetExpr) All() []Expr {
	return e.children
}

func (e *RuleSetExpr) Add(rule *RuleExpr) {
	e.children = append(e.children, rule)
}

func (e *RuleSetExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&RuleSetExpr{expr{op: RuleSetOp, children: children}})
	}
	return accept(e)
}

type RuleExpr struct{ expr }

func NewRuleExpr(header *RuleHeaderExpr, match Expr, replace Expr) *RuleExpr {
	children := []Expr{
		header,
		match,
		replace,
	}

	names := map[int]string{0: "Header", 1: "Match", 2: "Replace"}

	return &RuleExpr{expr{op: RuleOp, children: children, names: names}}
}

func (e *RuleExpr) Header() *RuleHeaderExpr {
	return e.children[0].(*RuleHeaderExpr)
}

func (e *RuleExpr) Match() Expr {
	return e.children[1]
}

func (e *RuleExpr) Replace() Expr {
	return e.children[2]
}

func (e *RuleExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&RuleExpr{expr{op: RuleOp, children: children, names: e.names}})
	}
	return accept(e)
}

type RuleHeaderExpr struct{ expr }

func NewRuleHeaderExpr(name string, tags []string) *RuleHeaderExpr {
	children := []Expr{
		NewStringExpr(name),
		NewTagsExpr(tags),
	}

	names := map[int]string{0: "Name", 1: "Tags"}

	return &RuleHeaderExpr{expr{op: RuleHeaderOp, children: children, names: names}}
}

func (e *RuleHeaderExpr) Name() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *RuleHeaderExpr) Tags() *TagsExpr {
	return e.children[1].(*TagsExpr)
}

func (e *RuleHeaderExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&RuleHeaderExpr{expr{op: RuleHeaderOp, children: children, names: e.names}})
	}
	return accept(e)
}

type BindExpr struct{ expr }

func NewBindExpr(label string, target Expr) *BindExpr {
	children := []Expr{
		NewStringExpr(label),
		target,
	}

	names := map[int]string{0: "Label", 1: "Target"}

	return &BindExpr{expr{op: BindOp, children: children, names: names}}
}

func (e *BindExpr) Label() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *BindExpr) Target() Expr {
	return e.children[1]
}

func (e *BindExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&BindExpr{expr{op: BindOp, children: children, names: e.names}})
	}
	return accept(e)
}

type RefExpr struct{ expr }

func NewRefExpr(label string) *RefExpr {
	children := []Expr{
		NewStringExpr(label),
	}

	names := map[int]string{0: "Label"}

	return &RefExpr{expr{op: RefOp, children: children, names: names}}
}

func (e *RefExpr) Label() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *RefExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&RefExpr{expr{op: RefOp, children: children, names: e.names}})
	}
	return accept(e)
}

type MatchTemplateExpr struct{ expr }

func NewMatchTemplateExpr(opNames *MatchTemplateNamesExpr) *MatchTemplateExpr {
	children := []Expr{
		opNames,
	}

	names := map[int]string{0: "Names"}

	return &MatchTemplateExpr{expr{op: MatchTemplateOp, children: children, names: names}}
}

func (e *MatchTemplateExpr) Names() *MatchTemplateNamesExpr {
	return e.children[0].(*MatchTemplateNamesExpr)
}

func (e *MatchTemplateExpr) Fields() []Expr {
	return e.children[1:]
}

func (e *MatchTemplateExpr) Add(match Expr) {
	e.children = append(e.children, match)
}

func (e *MatchTemplateExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchTemplateExpr{expr{op: MatchTemplateOp, children: children, names: e.names}})
	}
	return accept(e)
}

type MatchTemplateNamesExpr struct{ expr }

func NewMatchTemplateNamesExpr() *MatchTemplateNamesExpr {
	return &MatchTemplateNamesExpr{expr{op: MatchTemplateNamesOp}}
}

func (e *MatchTemplateNamesExpr) All() []Expr {
	return e.children
}

func (e *MatchTemplateNamesExpr) Add(name *StringExpr) {
	e.children = append(e.children, name)
}

func (e *MatchTemplateNamesExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchTemplateNamesExpr{expr{op: MatchTemplateNamesOp, children: children, names: e.names}})
	}
	return accept(e)
}

type MatchAndExpr struct{ expr }

func NewMatchAndExpr(left, right Expr) *MatchAndExpr {
	return &MatchAndExpr{expr{op: MatchAndOp, children: []Expr{left, right}}}
}

func (e *MatchAndExpr) Left() Expr {
	return e.children[0]
}

func (e *MatchAndExpr) Right() Expr {
	return e.children[1]
}

func (e *MatchAndExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchAndExpr{expr{op: MatchAndOp, children: children}})
	}
	return accept(e)
}

type MatchInvokeExpr struct{ expr }

func NewMatchInvokeExpr(funcName string) *MatchInvokeExpr {
	children := []Expr{
		NewStringExpr(funcName),
	}

	names := map[int]string{0: "FuncName"}

	return &MatchInvokeExpr{expr{op: MatchInvokeOp, children: children, names: names}}
}

func (e *MatchInvokeExpr) FuncName() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *MatchInvokeExpr) Args() []Expr {
	return e.children[1:]
}

func (e *MatchInvokeExpr) Add(match Expr) {
	e.children = append(e.children, match)
}

func (e *MatchInvokeExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchInvokeExpr{expr{op: MatchInvokeOp, children: children, names: e.names}})
	}
	return accept(e)
}

type MatchFieldsExpr struct{ expr }

func NewMatchFieldsExpr(opName string) *MatchFieldsExpr {
	children := []Expr{
		NewStringExpr(opName),
	}

	names := map[int]string{0: "OpName"}

	return &MatchFieldsExpr{expr{op: MatchFieldsOp, children: children, names: names}}
}

func (e *MatchFieldsExpr) OpName() string {
	return e.children[0].(*StringExpr).ValueAsString()
}

func (e *MatchFieldsExpr) Fields() []Expr {
	return e.children[1:]
}

func (e *MatchFieldsExpr) Add(match Expr) {
	e.children = append(e.children, match)
}

func (e *MatchFieldsExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchFieldsExpr{expr{op: MatchFieldsOp, children: children, names: e.names}})
	}
	return accept(e)
}

type MatchNotExpr struct{ expr }

func NewMatchNotExpr(input Expr) *MatchNotExpr {
	return &MatchNotExpr{expr{op: MatchNotOp, children: []Expr{input}}}
}

func (e *MatchNotExpr) Input() Expr {
	return e.children[0]
}

func (e *MatchNotExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchNotExpr{expr{op: MatchNotOp, children: children}})
	}
	return accept(e)
}

type MatchAnyExpr struct{ expr }

var matchAnySingleton = &MatchAnyExpr{expr{op: MatchAnyOp}}

func NewMatchAnyExpr() *MatchAnyExpr {
	return matchAnySingleton
}

func (e *MatchAnyExpr) Visit(accept AcceptFunc) Expr {
	return accept(e)
}

type MatchListExpr struct{ expr }

func NewMatchListExpr(matchItem Expr) *MatchListExpr {
	return &MatchListExpr{expr{op: MatchListOp, children: []Expr{matchItem}}}
}

func (e *MatchListExpr) MatchItem() Expr {
	return e.children[0]
}

func (e *MatchListExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&MatchListExpr{expr{op: MatchListOp, children: children, names: e.names}})
	}
	return accept(e)
}

type ReplaceRootExpr struct{ expr }

func NewReplaceRootExpr() *ReplaceRootExpr {
	return &ReplaceRootExpr{expr{op: ReplaceRootOp}}
}

func (e *ReplaceRootExpr) All() []Expr {
	return e.children
}

func (e *ReplaceRootExpr) Add(replace Expr) {
	e.children = append(e.children, replace)
}

func (e *ReplaceRootExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&ReplaceRootExpr{expr{op: ReplaceRootOp, children: children, names: e.names}})
	}
	return accept(e)
}

type ConstructExpr struct{ expr }

func NewConstructExpr(opName Expr) *ConstructExpr {
	children := []Expr{
		opName,
	}

	names := map[int]string{0: "OpName"}

	return &ConstructExpr{expr{op: ConstructOp, children: children, names: names}}
}

func (e *ConstructExpr) OpName() Expr {
	return e.children[0]
}

func (e *ConstructExpr) Args() []Expr {
	return e.children[1:]
}

func (e *ConstructExpr) Add(arg Expr) {
	e.children = append(e.children, arg)
}

func (e *ConstructExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&ConstructExpr{expr{op: ConstructOp, children: children, names: e.names}})
	}
	return accept(e)
}

type ConstructListExpr struct{ expr }

func NewConstructListExpr() *ConstructListExpr {
	return &ConstructListExpr{expr{op: ConstructListOp}}
}

func (e *ConstructListExpr) Add(item Expr) {
	e.children = append(e.children, item)
}

func (e *ConstructListExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&ConstructListExpr{expr{op: ConstructListOp, children: children, names: e.names}})
	}
	return accept(e)
}

type TagsExpr struct{ expr }

func NewTagsExpr(tags []string) *TagsExpr {
	e := &TagsExpr{expr{op: TagsOp}}

	for _, tag := range tags {
		e.children = append(e.children, NewStringExpr(tag))
	}

	return e
}

func (e *TagsExpr) All() []Expr {
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

func (e *TagsExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&TagsExpr{expr{op: TagsOp, children: children}})
	}
	return accept(e)
}

type StringExpr struct{ expr }

func NewStringExpr(s string) *StringExpr {
	return &StringExpr{expr{op: StringOp, value: s}}
}

func (e *StringExpr) ValueAsString() string {
	return e.value.(string)
}

func (e *StringExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&StringExpr{expr{op: StringOp, children: children}})
	}
	return accept(e)
}

type OpNameExpr struct{ expr }

func NewOpNameExpr(opName string) *OpNameExpr {
	return &OpNameExpr{expr{op: OpNameOp, value: opName}}
}

func (e *OpNameExpr) ValueAsOpName() string {
	return e.value.(string)
}

func (e *OpNameExpr) Visit(accept AcceptFunc) Expr {
	if children, replaced := e.visitChildren(accept); replaced {
		return accept(&OpNameExpr{expr{op: OpNameOp, children: children}})
	}
	return accept(e)
}

func writeIndent(buf *bytes.Buffer, level int) {
	buf.WriteString(strings.Repeat("  ", level))
}
