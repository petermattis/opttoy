package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"unicode"
)

var _ = fmt.Println

type Token int

const (
	ILLEGAL Token = iota

	EOF
	IDENT
	STRING
	WHITESPACE
	LPAREN
	RPAREN
	LBRACKET
	RBRACKET
	LBRACE
	RBRACE
	DOLLAR
	COLON
	ASTERISK
	EQUALS
	ARROW
	AMPERSANDS
	COMMA
	CARET
	ELLIPSES
	PIPE

	// Keywords.
	DEFINE
)

type LineInfo struct {
	Line int
	Pos  int
}

func (li LineInfo) MergeWith(other LineInfo) LineInfo {
	if li.Line < other.Line {
		return LineInfo{}
	}

	return LineInfo{}
}

type Scanner struct {
	r        *bufio.Reader
	tok      Token
	lit      string
	lineInfo struct {
		line int
		pos  int
		prev int
	}
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReader(r)}
}

func (s *Scanner) Token() Token {
	return s.tok
}

func (s *Scanner) Literal() string {
	return s.lit
}

func (s *Scanner) LineInfo() (line, pos int) {
	return s.lineInfo.line + 1, s.lineInfo.pos
}

func (s *Scanner) Scan() Token {
	// Read the next rune.
	ch := s.read()

	// If we see whitespace then consume all contiguous whitespace.
	if unicode.IsSpace(ch) {
		s.unread()
		return s.scanWhitespace()
	}

	// If we see a letter then consume as an identifier or keyword.
	if unicode.IsLetter(ch) {
		s.unread()
		return s.scanIdentifier()
	}

	// Otherwise read the individual character.
	switch ch {
	case rune(0):
		s.tok = EOF
		s.lit = ""

	case '(':
		s.tok = LPAREN
		s.lit = "("

	case ')':
		s.tok = RPAREN
		s.lit = ")"

	case '[':
		s.tok = LBRACKET
		s.lit = "["

	case ']':
		s.tok = RBRACKET
		s.lit = "]"

	case '{':
		s.tok = LBRACE
		s.lit = "{"

	case '}':
		s.tok = RBRACE
		s.lit = "}"

	case '$':
		s.tok = DOLLAR
		s.lit = "$"

	case ':':
		s.tok = COLON
		s.lit = ":"

	case '*':
		s.tok = ASTERISK
		s.lit = "*"

	case ',':
		s.tok = COMMA
		s.lit = ","

	case '^':
		s.tok = CARET
		s.lit = "^"

	case '|':
		s.tok = PIPE
		s.lit = "|"

	case '&':
		if s.read() == '&' {
			s.tok = AMPERSANDS
			s.lit = "&&"
			break
		}

		s.tok = ILLEGAL
		s.lit = "&"

	case '=':
		if s.read() == '>' {
			s.tok = ARROW
			s.lit = "=>"
			break
		}

		s.unread()
		s.tok = EQUALS
		s.lit = "="

	case '"':
		s.unread()
		return s.scanStringLiteral()

	case '.':
		if s.read() == '.' && s.read() == '.' {
			s.tok = ELLIPSES
			s.lit = "..."
			break
		}

		s.tok = ILLEGAL
		s.lit = "."

	default:
		s.tok = ILLEGAL
		s.lit = string(ch)
	}

	return s.tok
}

// read reads the next rune from the buffered reader.
// Returns the eof if an error occurs (or io.EOF is returned).
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return rune(0)
	}

	s.lineInfo.prev = s.lineInfo.pos
	if ch == '\n' {
		s.lineInfo.line++
		s.lineInfo.pos = 0
	} else {
		s.lineInfo.pos++
	}

	return ch
}

// unread places the previously read rune back on the reader.
func (s *Scanner) unread() {
	err := s.r.UnreadRune()
	if err != nil {
		panic(err)
	}

	s.tok = ILLEGAL
	s.lit = ""

	if s.lineInfo.pos == 0 {
		s.lineInfo.line--
	}

	s.lineInfo.pos = s.lineInfo.prev
	s.lineInfo.prev = -1
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		if ch := s.read(); ch == rune(0) {
			break
		} else if !unicode.IsSpace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	s.tok = WHITESPACE
	s.lit = buf.String()
	return WHITESPACE
}

// scanIdentifier consumes the current rune and all contiguous identifier runes.
func (s *Scanner) scanIdentifier() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read every subsequent ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	for {
		ch := s.read()
		if ch == rune(0) {
			break
		}

		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) {
			s.unread()
			break
		}

		buf.WriteRune(ch)
	}

	// If the string matches a keyword then return that keyword. Otherwise,
	// return as a regular identifier.
	switch buf.String() {
	case "define":
		s.tok = DEFINE

	default:
		s.tok = IDENT
	}

	s.lit = buf.String()
	return s.tok
}

func (s *Scanner) scanStringLiteral() Token {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
	buf.WriteRune(s.read())

	// Read characters until the closing quote is found, or until a newline or
	// EOF character is read.
	for {
		ch := s.read()
		if ch == rune(0) || ch == '\n' {
			s.tok = ILLEGAL
			break
		}

		buf.WriteRune(ch)

		if ch == '"' {
			s.tok = STRING
			break
		}
	}

	s.lit = buf.String()
	return s.tok
}
