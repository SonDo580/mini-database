package db

import (
	"errors"
	"fmt"
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

type NamedCell struct {
	column string
	value  Cell
}

type ExprAssign struct {
	column string
	expr   interface{} // *ExprUnOp | *ExprBinOp | string | *Cell
}

type StmtSelect struct {
	table string
	cols  []interface{} // *ExprUnOp | *ExprBinOp | string | *Cell
	cond  interface{}
}

type StmtCreateTable struct {
	table string
	cols  []Column
	pkey  []string
}

type StmtInsert struct {
	table string
	value []Cell
}

type StmtUpdate struct {
	table string
	cond  interface{}
	value []ExprAssign
}

type StmtDelete struct {
	table string
	cond  interface{}
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

func (p *Parser) tryKeyword(kws ...string) bool {
	saved := p.pos
	for _, kw := range kws {
		p.skipSpaces()
		if p.pos+len(kw) > len(p.buf) || !strings.EqualFold(kw, p.buf[p.pos:p.pos+len(kw)]) {
			p.pos = saved
			return false
		}
		if p.pos+len(kw) < len(p.buf) && !isSeparator(p.buf[p.pos+len(kw)]) {
			p.pos = saved
			return false
		}
		p.pos += len(kw)
	}

	return true
}

func (p *Parser) matchKeyword(kws ...string) error {
	if !p.tryKeyword(kws...) {
		expected := strings.ToUpper(strings.Join(kws, " "))
		return errors.New(fmt.Sprintf("expect keyword %q", expected))
	}
	return nil
}

func (p *Parser) tryPunctuation(tok string) bool {
	p.skipSpaces()
	if p.pos+len(tok) > len(p.buf) || p.buf[p.pos:p.pos+len(tok)] != tok {
		return false
	}

	p.pos += len(tok)
	return true
}

func (p *Parser) matchPunctuation(tok string) error {
	if !p.tryPunctuation(tok) {
		return errors.New(fmt.Sprintf("expect '%s'", tok))
	}
	return nil
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

func (p *Parser) parseStmt() (out any, err error) {
	if p.tryKeyword("SELECT") {
		stmt := &StmtSelect{}
		err = p.parseSelect(stmt)
		out = stmt
	} else if p.tryKeyword("CREATE", "TABLE") {
		stmt := &StmtCreateTable{}
		err = p.parseCreateTable(stmt)
		out = stmt
	} else if p.tryKeyword("INSERT", "INTO") {
		stmt := &StmtInsert{}
		err = p.parseInsert(stmt)
		out = stmt
	} else if p.tryKeyword("UPDATE") {
		stmt := &StmtUpdate{}
		err = p.parseUpdate(stmt)
		out = stmt
	} else if p.tryKeyword("DELETE", "FROM") {
		stmt := &StmtDelete{}
		err = p.parseDelete(stmt)
		out = stmt
	} else {
		err = errors.New("unknown statement")
	}

	if err != nil {
		return nil, err
	}
	return out, nil
}

func (p *Parser) parseSelect(out *StmtSelect) (err error) {
	for !p.tryKeyword("FROM") {
		if len(out.cols) > 0 {
			if err := p.matchPunctuation(","); err != nil {
				return err
			}
		}

		expr, err := p.parseExpr()
		if err != nil {
			return err
		}
		out.cols = append(out.cols, expr)
	}

	if len(out.cols) == 0 {
		return errors.New("expect list of expressions")
	}

	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}

	out.cond, err = p.parseWhere()
	return err
}

func (p *Parser) parseWhere() (expr interface{}, err error) {
	if err = p.matchKeyword("WHERE"); err != nil {
		return nil, err
	}
	if expr, err = p.parseExpr(); err != nil {
		return nil, err
	}
	if err = p.matchPunctuation(";"); err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) parseEqual(out *NamedCell) error {
	var ok bool
	out.column, ok = p.tryName()
	if !ok {
		return errors.New("expect column")
	}
	if err := p.matchPunctuation("="); err != nil {
		return err
	}
	return p.parseValue(&out.value)
}

func (p *Parser) parseAssign(out *ExprAssign) (err error) {
	var ok bool
	out.column, ok = p.tryName()
	if !ok {
		return errors.New("expect column")
	}
	if err = p.matchPunctuation("="); err != nil {
		return err
	}
	out.expr, err = p.parseExpr()
	return err
}

func (p *Parser) parseCommaList(parseItemFunc func() error) error {
	if err := p.matchPunctuation("("); err != nil {
		return err
	}

	needComma := false
	for !p.tryPunctuation(")") {
		if needComma {
			if err := p.matchPunctuation(","); err != nil {
				return err
			}
		}

		needComma = true
		if err := parseItemFunc(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Parser) parseCreateTablePKeyItem(out *[]string) error {
	name, ok := p.tryName()
	if !ok {
		return errors.New("expect column name")
	}
	*out = append(*out, name)
	return nil
}

func (p *Parser) parseCreateTableItem(out *StmtCreateTable) error {
	if p.tryKeyword("PRIMARY", "KEY") {
		return p.parseCommaList(func() error {
			return p.parseCreateTablePKeyItem(&out.pkey)
		})
	}

	var col Column
	var ok bool
	if col.Name, ok = p.tryName(); !ok {
		return errors.New("expect column name")
	}

	if p.tryKeyword("int64") {
		col.Type = TypeI64
	} else if p.tryKeyword("string") {
		col.Type = TypeStr
	} else {
		return errors.New("invalid column type")
	}

	out.cols = append(out.cols, col)
	return nil
}

func (p *Parser) parseCreateTable(out *StmtCreateTable) error {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}

	err := p.parseCommaList(func() error {
		return p.parseCreateTableItem(out)
	})
	if err != nil {
		return err
	}
	return p.matchPunctuation(";")
}

func (p *Parser) parseInsertValueItem(out *[]Cell) error {
	cell := Cell{}
	if err := p.parseValue(&cell); err != nil {
		return err
	}
	*out = append(*out, cell)
	return nil
}

func (p *Parser) parseInsert(out *StmtInsert) error {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}

	if err := p.matchKeyword("VALUES"); err != nil {
		return err
	}

	err := p.parseCommaList(func() error {
		return p.parseInsertValueItem(&out.value)
	})
	if err != nil {
		return err
	}
	return p.matchPunctuation(";")
}

func (p *Parser) parseUpdate(out *StmtUpdate) (err error) {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}

	if err = p.matchKeyword("SET"); err != nil {
		return err
	}
	for !p.tryKeyword("WHERE") {
		if len(out.value) > 0 {
			if err := p.matchPunctuation(","); err != nil {
				return err
			}
		}

		var expr ExprAssign
		if err := p.parseAssign(&expr); err != nil {
			return err
		}
		out.value = append(out.value, expr)
	}
	if len(out.value) == 0 {
		return errors.New("expect assignment list")
	}

	p.pos -= len("WHERE")
	out.cond, err = p.parseWhere()
	return err
}

func (p *Parser) parseDelete(out *StmtDelete) (err error) {
	var ok bool
	if out.table, ok = p.tryName(); !ok {
		return errors.New("expect table name")
	}
	out.cond, err = p.parseWhere()
	return err
}

type ExprOp uint8

/*
Operator precedence (lowest to highest):
. OR
. AND
. NOT
. comparison
. addition & subtraction
. multiplication & division
. negation
*/
const (
	OP_ADD ExprOp = 1  // +
	OP_SUB ExprOp = 2  // -
	OP_MUL ExprOp = 3  // *
	OP_DIV ExprOp = 4  // /
	OP_EQ  ExprOp = 10 // ==
	OP_NE  ExprOp = 11 // !=
	OP_LE  ExprOp = 12 // <=
	OP_GE  ExprOp = 13 // >=
	OP_LT  ExprOp = 14 // <
	OP_GT  ExprOp = 15 // >
	OP_AND ExprOp = 20 // AND
	OP_OR  ExprOp = 21 // OR
	OP_NOT ExprOp = 30 // NOT
	OP_NEG ExprOp = 31 // -
)

type ExprBinOp struct {
	op    ExprOp
	left  interface{}
	right interface{}
}

type ExprUnOp struct {
	op  ExprOp
	kid interface{}
}

func (p *Parser) parseAtom() (expr interface{}, err error) {
	// grouped expression
	if p.tryPunctuation("(") {
		if expr, err = p.parseExpr(); err != nil {
			return nil, err
		}
		if err = p.matchPunctuation(")"); err != nil {
			return nil, err
		}
		return expr, nil
	}

	// column name
	if name, ok := p.tryName(); ok {
		return name, nil
	}

	// constant value
	cell := &Cell{}
	if err = p.parseValue(cell); err != nil {
		return nil, err
	}
	return cell, nil
}

func (p *Parser) parseBinOp(
	tokens []string,
	ops []ExprOp,
	inner func() (interface{}, error), // function to parse the next precedence level
) (interface{}, error) {
	left, err := inner()
	if err != nil {
		return nil, err
	}

	for {
		matchedOp := false
		for i := range tokens {
			if !p.tryPunctuation(tokens[i]) && !p.tryKeyword(tokens[i]) {
				continue
			}

			matchedOp = true
			right, err := inner()
			if err != nil {
				return nil, err
			}

			// left-associative
			left = &ExprBinOp{op: ops[i], left: left, right: right}
			break
		}

		if !matchedOp {
			break
		}
	}

	return left, nil
}

func (p *Parser) parseExpr() (interface{}, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (interface{}, error) {
	return p.parseBinOp([]string{"OR"}, []ExprOp{OP_OR}, p.parseAnd)
}

func (p *Parser) parseAnd() (interface{}, error) {
	return p.parseBinOp([]string{"AND"}, []ExprOp{OP_AND}, p.parseNot)
}

func (p *Parser) parseNot() (expr interface{}, err error) {
	if p.tryKeyword("NOT") {
		if expr, err = p.parseNot(); err != nil {
			return nil, err
		}
		return &ExprUnOp{op: OP_NOT, kid: expr}, nil
	} else {
		return p.parseCmp()
	}
}

func (p *Parser) parseCmp() (expr interface{}, err error) {
	return p.parseBinOp(
		[]string{"=", "!=", "<>", "<=", ">=", "<", ">"},
		[]ExprOp{OP_EQ, OP_NE, OP_NE, OP_LE, OP_GE, OP_LT, OP_GT},
		p.parseAdd,
	)
}

/* Parse addition and subtraction. */
func (p *Parser) parseAdd() (interface{}, error) {
	return p.parseBinOp([]string{"+", "-"}, []ExprOp{OP_ADD, OP_SUB}, p.parseMul)
}

/* Parse multiplication and division. */
func (p *Parser) parseMul() (interface{}, error) {
	return p.parseBinOp([]string{"*", "/"}, []ExprOp{OP_MUL, OP_DIV}, p.parseNeg)
}

func (p *Parser) parseNeg() (expr interface{}, err error) {
	if p.tryPunctuation("-") {
		if expr, err = p.parseNeg(); err != nil {
			return nil, err
		}
		return &ExprUnOp{op: OP_NEG, kid: expr}, nil
	} else {
		return p.parseAtom()
	}
}
