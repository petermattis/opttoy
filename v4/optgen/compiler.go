package main

import (
	"fmt"
	"io"
)

type Compiler struct {
	s    *Scanner
	root *Expr
	err  error

	// True if the last token was unscanned (put back to be reparsed).
	unscanned bool
}

func NewCompiler(r io.Reader) *Compiler {
	return &Compiler{s: NewScanner(r)}
}

func (c *Compiler) Compile() (*RootExpr, error) {
	// Parse the input.
	root := c.parseRoot()
	if root == nil {
		return nil, c.err
	}

	// Semantically check the parse tree.
	c.checkTree(root)
	return root, c.err
}

func (c *Compiler) checkTree(root *RootExpr) {
	// Check definition semantics.
	for _, elem := range root.Defines().All() {
		define := elem.AsDefine()

		// Ensure that fields are defined in the following order:
		//   Expr*
		//   ExprList?
		//   Private?
		//
		// That is, there can be zero or more expression-typed fields, followed
		// by zero or one list-typed field, followed by zero or one private field.
		for i, elem2 := range define.Fields() {
			field := elem2.AsDefineField()
			if field.IsPrivateType() {
				if i != len(define.Fields())-1 {
					format := "private field '%s' is not the last field in '%s'"
					c.err = fmt.Errorf(format, field.Name(), define.Name())
					return
				}
			}
		}

		for i, elem2 := range define.Fields() {
			field := elem2.AsDefineField()
			if field.IsListType() {
				index := len(define.Fields()) - 1
				if define.Private() != nil {
					index--
				}

				if i != index {
					format := "list field '%s' is not the last non-private field in '%s'"
					c.err = fmt.Errorf(format, field.Name(), define.Name())
					return
				}
			}
		}
	}
}

func (c *Compiler) parseRoot() *RootExpr {
	rootOp := NewRootExpr()

	for {
		switch c.scan() {
		case DEFINE:
			c.unscan()

			define := c.parseDefine()
			if define == nil {
				return nil
			}

			rootOp.Defines().Add(define)

		case LBRACKET:
			c.unscan()

			rule := c.parseRule()
			if rule == nil {
				return nil
			}

			rootOp.Rules().Add(rule)

		case EOF:
			return rootOp

		default:
			c.setTokenErr(c.s.Literal())
			return nil
		}
	}
}

func (c *Compiler) parseDefine() *DefineExpr {
	if !c.scanToken(DEFINE) {
		return nil
	}

	if !c.scanToken(IDENT) {
		return nil
	}

	name := c.s.Literal()
	define := NewDefineExpr(name)

	if !c.scanToken(LBRACE) {
		return nil
	}

	for {
		if c.scan() == RBRACE {
			return define
		}

		c.unscan()
		define.Add(c.parseDefineField())
	}
}

func (c *Compiler) parseDefineField() *DefineFieldExpr {
	if !c.scanToken(IDENT) {
		return nil
	}

	name := c.s.Literal()

	if !c.scanToken(IDENT) {
		return nil
	}

	typ := c.s.Literal()

	return NewDefineFieldExpr(name, typ)
}

func (c *Compiler) parseRule() *RuleExpr {
	ruleHeader := c.parseRuleHeader()
	if ruleHeader == nil {
		return nil
	}

	matchFields := c.parseMatchFields()
	if matchFields == nil {
		return nil
	}

	if !c.scanToken(ARROW) {
		return nil
	}

	replace := c.parseReplace()
	if replace == nil {
		return nil
	}

	return NewRuleExpr(ruleHeader, matchFields, replace)
}

func (c *Compiler) parseRuleHeader() *RuleHeaderExpr {
	if !c.scanToken(LBRACKET) {
		return nil
	}

	if !c.scanToken(IDENT) {
		return nil
	}

	name := c.s.Literal()

	if !c.scanToken(RBRACKET) {
		return nil
	}

	return NewRuleHeaderExpr(name)
}

func (c *Compiler) parseMatch() *Expr {
	var matchList *MatchListExpr
	var match *Expr

	for {
		if match == nil {
			match = c.parseMatchItem()
		} else {
			if matchList == nil {
				matchList = NewMatchListExpr()
				matchList.Add(match)
				match = (*Expr)(matchList)
			}

			matchList.Add(c.parseMatchItem())
		}

		if c.scan() != AMPERSANDS {
			c.unscan()
			return match
		}
	}
}

func (c *Compiler) parseMatchItem() *Expr {
	for {
		switch c.scan() {
		case LPAREN:
			c.unscan()
			return (*Expr)(c.parseMatchFields())

		case STRING:
			c.unscan()
			return c.parseString()

		case DOLLAR:
			c.unscan()
			return c.parseBindMatchOrRef()

		case ASTERISK:
			return (*Expr)(NewMatchAnyExpr())

		default:
			c.setTokenErr(c.s.Literal())
			return nil
		}
	}
}

func (c *Compiler) parseMatchFields() *MatchFieldsExpr {
	if !c.scanToken(LPAREN) {
		return nil
	}

	if !c.scanToken(IDENT) {
		return nil
	}

	matchFields := NewMatchFieldsExpr(c.s.Literal())

	for {
		if c.scan() == RPAREN {
			return matchFields
		}

		c.unscan()
		match := c.parseMatch()
		if match == nil {
			return nil
		}

		matchFields.Add(match)
	}
}

func (c *Compiler) parseString() *Expr {
	if !c.scanToken(STRING) {
		return nil
	}

	// Strip quotes.
	s := c.s.Literal()
	s = s[1 : len(s)-1]

	return (*Expr)(NewStringExpr(s))
}

func (c *Compiler) parseBindMatchOrRef() *Expr {
	if !c.scanToken(DOLLAR) {
		return nil
	}

	if !c.scanToken(IDENT) {
		return nil
	}

	label := c.s.Literal()

	if c.scan() != COLON {
		c.unscan()
		return (*Expr)(NewRefExpr(label))
	}

	target := c.parseMatchItem()
	return (*Expr)(NewBindExpr(label, target))
}

func (c *Compiler) parseReplace() *Expr {
	var replaceList *ReplaceListExpr
	var replace *Expr

	for {
		switch c.scan() {
		case LPAREN:
			fallthrough

		case STRING:
			fallthrough

		case DOLLAR:
			c.unscan()

			if replace == nil {
				replace = c.parseReplaceItem()
			} else {
				if replaceList == nil {
					replaceList = NewReplaceListExpr()
					replaceList.Add(replace)
					replace = (*Expr)(replaceList)
				}

				replaceList.Add(c.parseReplaceItem())
			}

		default:
			c.unscan()
			return (*Expr)(replace)
		}
	}
}

func (c *Compiler) parseReplaceItem() *Expr {
	switch c.scan() {
	case LPAREN:
		c.unscan()
		return (*Expr)(c.parseConstruct())

	case DOLLAR:
		c.unscan()
		return (*Expr)(c.parseRef())

	case STRING:
		c.unscan()
		return (*Expr)(c.parseString())

	default:
		c.setTokenErr(c.s.Literal())
		return nil
	}
}

func (c *Compiler) parseConstruct() *ConstructExpr {
	if !c.scanToken(LPAREN) {
		return nil
	}

	if !c.scanToken(IDENT) {
		return nil
	}

	replaceResult := NewConstructExpr(c.s.Literal())

	for {
		if c.scan() == RPAREN {
			return replaceResult
		}

		c.unscan()
		item := c.parseReplaceItem()
		if item == nil {
			return nil
		}

		replaceResult.Add(item)
	}
}

func (c *Compiler) parseRef() *Expr {
	if !c.scanToken(DOLLAR) {
		return nil
	}

	if !c.scanToken(IDENT) {
		return nil
	}

	return (*Expr)(NewRefExpr(c.s.Literal()))
}

func (c *Compiler) scanToken(expected Token) bool {
	if c.scan() != expected {
		c.setTokenErr(c.s.Literal())
		c.unscan()
		return false
	}

	return true
}

// scan returns the next non-whitespace token from the underlying scanner. If
// a token has been unscanned then read that instead.
func (c *Compiler) scan() Token {
	// If we have a token on the buffer, then return it.
	if c.unscanned {
		c.unscanned = false
		return c.s.Token()
	}

	// Otherwise read the next token from the scanner.
	for {
		tok := c.s.Scan()

		if tok != WHITESPACE {
			return tok
		}
	}
}

// unscan pushes the previously read token back onto the buffer.
func (c *Compiler) unscan() {
	if c.unscanned {
		panic("unscan was already called")
	}

	c.unscanned = true
}

func (c *Compiler) setTokenErr(lit string) {
	line, pos := c.s.LineInfo()
	c.err = fmt.Errorf("unexpected token '%s' (line %d, pos %d)", lit, line, pos)
}
