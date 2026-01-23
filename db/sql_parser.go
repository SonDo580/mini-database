package db

import "strings"

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
