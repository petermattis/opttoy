package main

import (
	"fmt"
	"io"
)

type Parser struct {
	s   *Scanner
	err error

	// True if the last token was unscanned (put back to be reparsed).
	unscanned bool
}

func NewParser(r io.Reader) *Parser {
	return &Parser{s: NewScanner(r)}
}

func (p *Parser) Parse() (*RootExpr, error) {
	return p.parseRoot(), p.err
}

func (p *Parser) parseRoot() *RootExpr {
	rootOp := NewRootExpr()

	for {
		var tags []string

		switch p.scan() {
		case LBRACKET:
			p.unscan()

			tags = p.parseTags()
			if tags == nil {
				return nil
			}

			if p.scan() != DEFINE {
				p.unscan()

				rule := p.parseRule(tags)
				if rule == nil {
					return nil
				}

				rootOp.Rules().Add(rule)
				break
			}

			fallthrough

		case DEFINE:
			p.unscan()

			define := p.parseDefine(tags)
			if define == nil {
				return nil
			}

			rootOp.Defines().Add(define)

		case EOF:
			return rootOp

		default:
			p.setTokenErr(p.s.Literal())
			return nil
		}
	}
}

func (p *Parser) parseDefine(tags []string) *DefineExpr {
	if !p.scanToken(DEFINE) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	name := p.s.Literal()
	define := NewDefineExpr(name, tags)

	if !p.scanToken(LBRACE) {
		return nil
	}

	for {
		if p.scan() == RBRACE {
			return define
		}

		p.unscan()
		define.Add(p.parseDefineField())
	}
}

func (p *Parser) parseDefineField() *DefineFieldExpr {
	if !p.scanToken(IDENT) {
		return nil
	}

	name := p.s.Literal()

	if !p.scanToken(IDENT) {
		return nil
	}

	typ := p.s.Literal()

	return NewDefineFieldExpr(name, typ)
}

func (p *Parser) parseRule(tags []string) *RuleExpr {
	ruleHeader := NewRuleHeaderExpr(tags[0], tags[1:])

	match := p.parseMatchTemplate()
	if match == nil {
		return nil
	}

	if !p.scanToken(ARROW) {
		return nil
	}

	replace := p.parseReplace()
	if replace == nil {
		return nil
	}

	return NewRuleExpr(ruleHeader, match, replace)
}

func (p *Parser) parseMatchTemplate() ParsedExpr {
	if !p.scanToken(LPAREN) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	templateNames := NewMatchTemplateNamesExpr()
	for {
        templateNames.Add(NewStringExpr(p.s.Literal()))

		if p.scan() != PIPE {
			p.unscan()
			break
		}

		if !p.scanToken(IDENT) {
        	return nil
		}
	}

	template := NewMatchTemplateExpr(templateNames)
	for {
		if p.scan() == RPAREN {
			return template
		}

		p.unscan()
		match := p.parseMatchFieldsArg()
		if match == nil {
			return nil
		}

		template.Add(match)
	}
}

func (p *Parser) parseMatchFields() *MatchFieldsExpr {
	if !p.scanToken(LPAREN) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	matchFields := NewMatchFieldsExpr(p.s.Literal())

	for {
		if p.scan() == RPAREN {
			return matchFields
		}

		p.unscan()
		match := p.parseMatchFieldsArg()
		if match == nil {
			return nil
		}

		matchFields.Add(match)
	}
}

func (p *Parser) parseMatchFieldsArg() ParsedExpr {
	tok := p.scan()
	p.unscan()

	var match ParsedExpr
	if tok == DOLLAR {
		match = p.parseMatchBind()
	} else {
		match = p.parseMatchExpr()
	}

	if p.scan() != AMPERSAND {
		p.unscan()
		return match
	}

	return NewMatchAndExpr(match, p.parseMatchAndExpr())
}

func (p *Parser) parseMatchExpr() ParsedExpr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseMatchFields()

	case STRING:
		p.unscan()
		return p.parseString()

	case CARET:
		input := p.parseMatchExpr()
		return NewMatchNotExpr(input)

	case ASTERISK:
		return NewMatchAnyExpr()

	case LBRACKET:
		p.unscan()
		return p.parseMatchListExpr()

	default:
		p.setTokenErr(p.s.Literal())
		return nil
	}
}

func (p *Parser) parseMatchBind() *BindExpr {
	if !p.scanToken(DOLLAR) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	label := p.s.Literal()

	if !p.scanToken(COLON) {
		return nil
	}

	target := p.parseMatchExpr()

	return NewBindExpr(label, target)
}

func (p *Parser) parseMatchAndExpr() ParsedExpr {
	match := p.parseMatchNotExpr()

	if p.scan() != AMPERSAND {
		p.unscan()
		return match
	}

	return NewMatchAndExpr(match, p.parseMatchAndExpr())
}

func (p *Parser) parseMatchNotExpr() ParsedExpr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseMatchInvoke()

	case CARET:
		input := p.parseMatchNotExpr()
		return NewMatchNotExpr(input)

	default:
		p.setTokenErr(p.s.Literal())
		return nil
	}
}

func (p *Parser) parseMatchInvoke() *MatchInvokeExpr {
	if !p.scanToken(LPAREN) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	matchInvoke := NewMatchInvokeExpr(p.s.Literal())

	for {
		switch p.scan() {
		case RPAREN:
			return matchInvoke

		case DOLLAR:
			p.unscan()
			ref := p.parseRef()
			if ref == nil {
				return nil
			}

			matchInvoke.Add(ref)

		default:
			p.setTokenErr(p.s.Literal())
			return nil
		}
	}
}

func (p *Parser) parseMatchListExpr() ParsedExpr {
	if !p.scanToken(LBRACKET) {
		return nil
	}

	if !p.scanToken(ELLIPSES) {
		return nil
	}

	matchItem := p.parseMatchBind()

	if !p.scanToken(ELLIPSES) {
		return nil
	}

	if !p.scanToken(RBRACKET) {
		return nil
	}

	return NewMatchListExpr(matchItem)
}

func (p *Parser) parseReplace() ParsedExpr {
	replaceRoot := NewReplaceRootExpr()

	for {
		switch p.scan() {
		case LPAREN:
			fallthrough

		case STRING:
			fallthrough

		case DOLLAR:
			p.unscan()
			replace := p.parseReplaceItem()
			replaceRoot.Add(replace)

		default:
			p.unscan()
			switch len(replaceRoot.All()) {
			case 0:
				// Must be at least one replace expression.
				p.setTokenErr(p.s.Literal())
				return nil

			case 1:
				return replaceRoot.All()[0]
			}

			return replaceRoot
		}
	}
}

func (p *Parser) parseReplaceItem() ParsedExpr {
	switch p.scan() {
	case LPAREN:
		p.unscan()
		return p.parseConstruct()

	case LBRACKET:
		p.unscan()
		return p.parseConstructList()

	case DOLLAR:
		p.unscan()
		return p.parseRef()

	case STRING:
		p.unscan()
		return p.parseString()

	default:
		p.setTokenErr(p.s.Literal())
		return nil
	}
}

func (p *Parser) parseConstruct() *ConstructExpr {
	if !p.scanToken(LPAREN) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	replaceResult := NewConstructExpr(p.s.Literal())

	for {
		if p.scan() == RPAREN {
			return replaceResult
		}

		p.unscan()
		item := p.parseReplaceItem()
		if item == nil {
			return nil
		}

		replaceResult.Add(item)
	}
}

func (p *Parser) parseConstructList() *ConstructListExpr {
	if !p.scanToken(LBRACKET) {
		return nil
	}

	replaceResult := NewConstructListExpr()

	for {
		if p.scan() == RBRACKET {
			return replaceResult
		}

		p.unscan()
		item := p.parseReplaceItem()
		if item == nil {
			return nil
		}

		replaceResult.Add(item)
	}
}

func (p *Parser) parseRef() *RefExpr {
	if !p.scanToken(DOLLAR) {
		return nil
	}

	if !p.scanToken(IDENT) {
		return nil
	}

	return NewRefExpr(p.s.Literal())
}

func (p *Parser) parseString() *StringExpr {
	if !p.scanToken(STRING) {
		return nil
	}

	// Strip quotes.
	s := p.s.Literal()
	s = s[1 : len(s)-1]

	return NewStringExpr(s)
}

func (p *Parser) parseTags() []string {
	var tags []string

	if !p.scanToken(LBRACKET) {
		return nil
	}

	for {
		if !p.scanToken(IDENT) {
			return nil
		}

		tags = append(tags, p.s.Literal())

		if p.scan() == RBRACKET {
			return tags
		}

		p.unscan()
		if !p.scanToken(COMMA) {
			return nil
		}
	}
}

func (p *Parser) scanToken(expected Token) bool {
	if p.scan() != expected {
		p.setTokenErr(p.s.Literal())
		p.unscan()
		return false
	}

	return true
}

// scan returns the next non-whitespace, non-comment token from the underlying
// scanner. If a token has been unscanned then read that instead.
func (p *Parser) scan() Token {
	// If we have a token on the buffer, then return it.
	if p.unscanned {
		p.unscanned = false
		return p.s.Token()
	}

	// Otherwise read the next token from the scanner.
	for {
		tok := p.s.Scan()

		if tok != WHITESPACE && tok != COMMENT {
			return tok
		}
	}
}

// unscan pushes the previously read token back onto the buffer.
func (p *Parser) unscan() {
	if p.unscanned {
		panic("unscan was already called")
	}

	p.unscanned = true
}

func (p *Parser) setTokenErr(lit string) {
	line, pos := p.s.LineInfo()
	p.err = fmt.Errorf("unexpected token '%s' (line %d, pos %d)", lit, line, pos)
}
