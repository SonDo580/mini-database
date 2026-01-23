package db

import (
	"errors"
	"strconv"
	"strings"
)

type Parser struct {
	buf string
	pos int
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

func isSpace(ch byte) bool {
	switch ch {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

func isAlpha(ch byte) bool {
	// A	65		01000001
	// ...
	// Z	90		01011010
	// a	97		01100001
	// ...
	// z	122		01111010
	// -> A | 2^5 == a, ..., Z | 2^5 == z
	return 'a' <= (ch|32) && (ch|32) <= 'z'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isNameStart(ch byte) bool {
	return isAlpha(ch) || ch == '_'
}

func isNameContinue(ch byte) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '_'
}

func isSeparator(ch byte) bool {
	// 0 -> 127: Standard ASCII
	return ch < 128 && !isNameContinue(ch)
}

func (p *Parser) skipSpaces() {
	for p.pos < len(p.buf) && isSpace(p.buf[p.pos]) {
		p.pos++
	}
}

func (p *Parser) isEnd() bool {
	p.skipSpaces()
	return p.pos == len(p.buf)
}

func (p *Parser) tryName() (string, bool) {
	p.skipSpaces()
	start, curr := p.pos, p.pos

	if curr == len(p.buf) || !isNameStart(p.buf[curr]) {
		return "", false
	}
	curr++

	for curr < len(p.buf) && isNameContinue(p.buf[curr]) {
		curr++
	}

	p.pos = curr
	return p.buf[start:curr], true
}

func (p *Parser) tryKeyword(kw string) bool {
	p.skipSpaces()

	if p.pos+len(kw) > len(p.buf) || !strings.EqualFold(kw, p.buf[p.pos:p.pos+len(kw)]) {
		return false
	}
	if p.pos+len(kw) < len(p.buf) && !isSeparator(p.buf[p.pos+len(kw)]) {
		return false
	}

	p.pos += len(kw)
	return true
}

func (p *Parser) parseValue(out *Cell) error {
	p.skipSpaces()
	if p.pos == len(p.buf) {
		return errors.New("expect value")
	}

	ch := p.buf[p.pos]
	if ch == '"' || ch == '\'' {
		return p.parseString(out)
	}
	if isDigit(ch) || ch == '-' || ch == '+' {
		return p.parseInt(out)
	}
	return errors.New("expect value")
}

func (p *Parser) parseString(out *Cell) error {
	start, curr := p.pos, p.pos
	quote := p.buf[start]
	curr++ // skip opening quote

	str_bytes := []byte{}
	for curr < len(p.buf) && p.buf[curr] != quote {
		if p.buf[curr] == '\\' {
			// escaped characters: \\, \', \" -> skip the leading \
			str_bytes = append(str_bytes, p.buf[curr+1])
			curr += 2
		} else {
			str_bytes = append(str_bytes, p.buf[curr])
			curr++
		}
	}

	if curr >= len(p.buf) {
		return errors.New("expect string")
	}

	p.pos = curr + 1 // skip closing quote
	out.Type = TypeStr
	out.Str = str_bytes
	return nil
}

func (p *Parser) parseInt(out *Cell) error {
	start, curr := p.pos, p.pos
	curr++ // match the first digit or '-' or '+'

	for curr < len(p.buf) && isDigit(p.buf[curr]) {
		curr++
	}

	val, err := strconv.ParseInt(p.buf[start:curr], 10, 64)
	if err != nil {
		return err
	}

	p.pos = curr
	out.Type = TypeI64
	out.I64 = val
	return nil
}
