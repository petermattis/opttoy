package main

import (
	"bytes"
	"fmt"
	"io"
)

type CompiledExpr interface {
	Defines() []*DefineExpr
	Rules() []*RuleExpr
	DefineTags() []string
	LookupDefine(opName string) *DefineExpr
	String() string
}

type compiledExpr struct {
	root    *RootExpr
	defines []*DefineExpr
	rules   []*RuleExpr
	defTags []string
	opIndex map[string]*DefineExpr
}

func (c *compiledExpr) Defines() []*DefineExpr {
	return c.defines
}

func (c *compiledExpr) Rules() []*RuleExpr {
	return c.rules
}

func (c *compiledExpr) DefineTags() []string {
	return c.defTags
}

func (c *compiledExpr) LookupDefine(opName string) *DefineExpr {
	return c.opIndex[opName]
}

func (c *compiledExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("(Compiled")
	c.root.Format(&buf, 1)
	buf.WriteByte(')')
	return buf.String()
}

type Compiler struct {
	parser   *Parser
	compiled *compiledExpr
	err      error
}

func NewCompiler(r io.Reader) *Compiler {
	compiled := &compiledExpr{opIndex: make(map[string]*DefineExpr)}
	return &Compiler{parser: NewParser(r), compiled: compiled}
}

func (c *Compiler) Compile() (CompiledExpr, error) {
	c.compiled.root, c.err = c.parser.Parse()
	if c.err != nil {
		return nil, c.err
	}

	if !c.compileDefines() {
		return c.compiled, c.err
	}

	if !c.compileRules() {
		return c.compiled, c.err
	}

	return c.compiled, c.err
}

func (c *Compiler) compileDefines() bool {
	tags := make(map[string]bool)

	for _, elem := range c.compiled.root.Defines().All() {
		define := elem.(*DefineExpr)

		// Determine set of unique tags.
		for _, elem2 := range define.Tags().All() {
			tag := elem2.(*StringExpr).ValueAsString()
			if !tags[tag] {
				c.compiled.defTags = append(c.compiled.defTags, tag)
				tags[tag] = true
			}
		}

		// Record the define in the index for fast lookup.
		c.compiled.opIndex[define.Name()] = define

		// Ensure that fields are defined in the following order:
		//   Expr*
		//   ExprList?
		//   Private?
		//
		// That is, there can be zero or more expression-typed fields, followed
		// by zero or one list-typed field, followed by zero or one private field.
		for i, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsPrivateType() {
				if i != len(define.Fields())-1 {
					format := "private field '%s' is not the last field in '%s'"
					c.err = fmt.Errorf(format, field.Name(), define.Name())
					return false
				}
			}
		}

		for i, elem2 := range define.Fields() {
			field := elem2.(*DefineFieldExpr)
			if field.IsListType() {
				index := len(define.Fields()) - 1
				if define.PrivateField() != nil {
					index--
				}

				if i != index {
					format := "list field '%s' is not the last non-private field in '%s'"
					c.err = fmt.Errorf(format, field.Name(), define.Name())
					return false
				}
			}
		}

		c.compiled.defines = append(c.compiled.defines, define)
	}

	return true
}

func (c *Compiler) compileRules() bool {
	for _, elem := range c.compiled.root.Rules().All() {
		rule := elem.(*RuleExpr)
		if !c.compileRuleMatchExpr(rule.Match()) {
			return false
		}

		// Expand rule templates into one or more field match expressions.
		template := rule.Match().(*MatchTemplateExpr)
		for _, elem2 := range template.Names().Children() {
			name := elem2.(*StringExpr).ValueAsString()

			def := c.compiled.LookupDefine(name)
			if def != nil {
				// Name is an op name, so create a rule for the op.
				c.expandTemplate(rule, template, name)
			} else {
				// Name must be a tag name, so find all defines with that tag.
				found := false
				for _, define := range c.compiled.Defines() {
					if define.HasTag(name) {
						c.expandTemplate(rule, template, define.Name())
						found = true
						break
					}
				}

				if !found {
					c.err = fmt.Errorf("unrecognized match name '%s'", name)
					return false
				}
			}
		}
	}

	return true
}

func (c *Compiler) expandTemplate(rule *RuleExpr, template *MatchTemplateExpr, opName string) {
	matchFields := NewMatchFieldsExpr(opName)
	for _, match := range template.Fields() {
		matchFields.Add(match)
	}

	newRule := NewRuleExpr(rule.Header(), matchFields, rule.Replace())
	c.compiled.rules = append(c.compiled.rules, newRule)
}

func (c *Compiler) compileRuleMatchExpr(expr ParsedExpr) bool {
	for _, child := range expr.Children() {
		if !c.compileRuleMatchExpr(child) {
			return false
		}
	}

	return true
}
