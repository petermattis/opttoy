package main

import (
	"bytes"
	"fmt"
	"io"
)

type CompiledExpr interface {
	Root() *RootExpr
	DefinitionTags() []string
	LookupDefinition(opName string) *DefineExpr
	String() string
}

type compiledExpr struct {
	root    *RootExpr
	defTags []string
	opIndex map[string]*DefineExpr
}

func (c *compiledExpr) Root() *RootExpr {
	return c.root
}

func (c *compiledExpr) DefinitionTags() []string {
	return c.defTags
}

func (c *compiledExpr) LookupDefinition(opName string) *DefineExpr {
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

	if !c.compileDefinitions() {
		return c.compiled, c.err
	}

	if !c.compileRules() {
		return c.compiled, c.err
	}

	return c.compiled, c.err
}

func (c *Compiler) compileDefinitions() bool {
	tags := make(map[string]bool)

	for _, elem := range c.compiled.root.Defines().All() {
		define := elem.(*DefineExpr)

		// Determine set of unique tags.
		for _, elem2 := range define.Tags().All() {
			tag := elem2.(*StringExpr).Value()
			if !tags[tag] {
				c.compiled.defTags = append(c.compiled.defTags, tag)
				tags[tag] = true
			}
		}

		// Record the definition in the index for fast lookup.
		c.compiled.opIndex[define.Name()] = define

		// Ensure that fields are defined in the following order:
		//   expr*
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
	}

	return true
}

func (c *Compiler) compileRules() bool {
	for _, elem := range c.compiled.root.Rules().All() {
		rule := elem.(*RuleExpr)
		if !c.compileRuleMatchExpr(rule.Match()) {
			return false
		}
	}

	return true
}

func (c *Compiler) compileRuleMatchExpr(expr ParsedExpr) bool {
	for _, child := range expr.Children() {
		if !c.compileRuleMatchExpr(child) {
			return false
		}
	}

	return true
}
