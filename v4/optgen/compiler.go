package optgen

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

func (c *compiledExpr) LookupDefine(name string) *DefineExpr {
	return c.opIndex[name]
}

func (c *compiledExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("(Compiled\n")
	writeIndent(&buf, 1)
	c.root.Format(&buf, 1)
	buf.WriteString("\n)")
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
		var ruleCompiler ruleCompiler
		c.err = ruleCompiler.compile(c.compiled, elem.(*RuleExpr))
		if c.err != nil {
			return false
		}
	}
	return true
}

type ruleCompiler struct {
	compiled  *compiledExpr
	rule      *RuleExpr
	matchRoot *MatchFieldsExpr
	opName    *OpNameExpr
	err       error
}

func (c *ruleCompiler) compile(compiled *compiledExpr, rule *RuleExpr) error {
	c.compiled = compiled
	c.rule = rule
	c.matchRoot = c.rule.Match().(*MatchFieldsExpr)

	// Expand root rules that match multiple operators into a separate field
	// match expression for each matching operator.
	for _, elem2 := range c.matchRoot.Names().(*MatchNamesExpr).All() {
		name := elem2.(*StringExpr).ValueAsString()

		def := c.compiled.LookupDefine(name)
		if def != nil {
			// Name is an op name, so create a rule for the op.
			if !c.expandRule(NewOpNameExpr(name)) {
				return c.err
			}
		} else {
			// Name must be a tag name, so find all defines with that tag.
			found := false
			for _, define := range c.compiled.Defines() {
				if define.HasTag(name) {
					if !c.expandRule(NewOpNameExpr(define.Name())) {
						return c.err
					}
					found = true
				}
			}

			if !found {
				return fmt.Errorf("unrecognized match name '%s'", name)
			}
		}
	}

	return nil
}

func (c *ruleCompiler) expandRule(opName *OpNameExpr) bool {
	c.opName = opName

	// Construct new root expression that matches a single name.
	matchFields := NewMatchFieldsExpr(opName)
	for _, match := range c.matchRoot.Fields() {
		matchFields.Add(match)
	}

	match := matchFields.Visit(c.acceptRuleMatchExpr)
	if c.err != nil {
		return false
	}

	replace := c.rule.Replace().Visit(c.acceptRuleReplaceExpr)
	if c.err != nil {
		return false
	}

	newRule := NewRuleExpr(c.rule.Header(), match, replace)
	c.compiled.rules = append(c.compiled.rules, newRule)

	return c.err == nil
}

func (c *ruleCompiler) acceptRuleMatchExpr(expr Expr) Expr {
	if matchNames, ok := expr.(*MatchNamesExpr); ok {
		// Create a constant name expression if there's a single name that
		// matches a define name.
		if len(matchNames.All()) == 1 {
			name := matchNames.Name(0)
			if c.compiled.LookupDefine(name) != nil {
				return NewOpNameExpr(name)
			}
		}
	}

	return expr
}

func (c *ruleCompiler) acceptRuleReplaceExpr(expr Expr) Expr {
	if construct, ok := expr.(*ConstructExpr); ok {
		// Handle built-in OpName function.
		if strName, ok := construct.OpName().(*StringExpr); ok && strName.ValueAsString() == "OpName" {
			if len(construct.Args()) > 1 {
				c.err = fmt.Errorf("too many arguments to OpName function: %v", strName)
				return expr
			}

			if len(construct.Args()) == 0 {
				// No args to OpName function refers to top-level match operator.
				return c.opName
			}

			// Otherwise accept a single variable reference argument.
			ref, ok := construct.Args()[0].(*RefExpr)
			if !ok {
				c.err = fmt.Errorf("invalid argument to OpName function: %v", construct.Args()[0])
				return expr
			}

			// Get the match name of the expression bound to the variable.
			opName := c.resolveOpName(c.matchRoot, ref.Label())
			if opName != nil {
				return opName
			}
		}
	}

	return expr
}

func (c *ruleCompiler) resolveOpName(expr Expr, label string) *OpNameExpr {
	if bind, ok := expr.(*BindExpr); ok {
		if bind.Label() == label {
			if matchFields, ok := bind.Target().(*MatchFieldsExpr); ok {
				if opName, ok := matchFields.Names().(*OpNameExpr); ok {
					return opName
				}
			} else {
				c.err = fmt.Errorf("invalid OpName parameter: $%s must be bound to a match expression", label)
			}
			return nil
		}
	}

	for _, child := range expr.Children() {
		if name := c.resolveOpName(child, label); name != nil {
			return name
		}
	}

	return nil
}
