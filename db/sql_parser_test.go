package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseName(t *testing.T) {
	p := NewParser(" a  b0 _0_ 123 ")
	name, ok := p.tryName()
	assert.True(t, ok && name == "a")
	name, ok = p.tryName()
	assert.True(t, ok && name == "b0")
	name, ok = p.tryName()
	assert.True(t, ok && name == "_0_")
	_, ok = p.tryName()
	assert.False(t, ok)
}

func TestParseKeyword(t *testing.T) {
	p := NewParser(" select  HELLO ")
	assert.False(t, p.tryKeyword("sel"))
	assert.True(t, p.tryKeyword("SELECT"))
	assert.True(t, p.tryKeyword("Hello") && p.isEnd())
}

func testParseValue(t *testing.T, s string, ref Cell) {
	p := NewParser(s)
	out := Cell{}
	err := p.parseValue(&out)
	assert.Nil(t, err)
	assert.True(t, p.isEnd())
	assert.Equal(t, ref, out)
}

func TestParseValue(t *testing.T) {
	testParseValue(t, " 123 ", Cell{Type: TypeI64, I64: 123})
	testParseValue(t, " +123 ", Cell{Type: TypeI64, I64: 123})
	testParseValue(t, " -123 ", Cell{Type: TypeI64, I64: -123})
	testParseValue(t, ` 'abc\'\"d' `, Cell{Type: TypeStr, Str: []byte("abc'\"d")})
	testParseValue(t, ` "abc\'\"d" `, Cell{Type: TypeStr, Str: []byte("abc'\"d")})
}

func testParseStmt(t *testing.T, s string, ref any) {
	p := NewParser(s)
	out, err := p.parseStmt()
	assert.Nil(t, err)
	assert.True(t, p.isEnd())
	assert.Equal(t, ref, out)
}

func TestParseStmt(t *testing.T) {
	var stmt any

	s := "select a from t where c=1;"
	stmt = &StmtSelect{
		table: "t",
		cols:  []string{"a"},
		keys:  []NamedCell{{column: "c", value: Cell{Type: TypeI64, I64: 1}}},
	}
	testParseStmt(t, s, stmt)

	s = "select a, b_02 from t where c=1 and d='e';"
	stmt = &StmtSelect{
		table: "t",
		cols:  []string{"a", "b_02"},
		keys: []NamedCell{
			{column: "c", value: Cell{Type: TypeI64, I64: 1}},
			{column: "d", value: Cell{Type: TypeStr, Str: []byte("e")}},
		},
	}
	testParseStmt(t, s, stmt)

	s = "create table t (a string, b int64, primary key (b));"
	stmt = &StmtCreateTable{
		table: "t",
		cols: []Column{
			{Name: "a", Type: TypeStr},
			{Name: "b", Type: TypeI64},
		},
		pkey: []string{"b"},
	}
	testParseStmt(t, s, stmt)

	s = "insert into t values (1, 'hi');"
	stmt = &StmtInsert{
		table: "t",
		value: []Cell{
			{Type: TypeI64, I64: 1},
			{Type: TypeStr, Str: []byte("hi")},
		},
	}
	testParseStmt(t, s, stmt)

	s = "update t set a = 1, b = 'hi' where c = 3 and d = 'x';"
	stmt = &StmtUpdate{
		table: "t",
		value: []NamedCell{
			{column: "a", value: Cell{Type: TypeI64, I64: 1}},
			{column: "b", value: Cell{Type: TypeStr, Str: []byte("hi")}},
		},
		keys: []NamedCell{
			{column: "c", value: Cell{Type: TypeI64, I64: 3}},
			{column: "d", value: Cell{Type: TypeStr, Str: []byte("x")}},
		},
	}
	testParseStmt(t, s, stmt)

	s = "update t set a = 1, b = 'hi' where c = 3 ;"
	stmt = &StmtUpdate{
		table: "t",
		value: []NamedCell{
			{column: "a", value: Cell{Type: TypeI64, I64: 1}},
			{column: "b", value: Cell{Type: TypeStr, Str: []byte("hi")}},
		},
		keys: []NamedCell{
			{column: "c", value: Cell{Type: TypeI64, I64: 3}},
		},
	}
	testParseStmt(t, s, stmt)

	s = "delete from t where c = 3 and d = 4;"
	stmt = &StmtDelete{
		table: "t",
		keys: []NamedCell{
			{column: "c", value: Cell{Type: TypeI64, I64: 3}},
			{column: "d", value: Cell{Type: TypeI64, I64: 4}},
		},
	}
	testParseStmt(t, s, stmt)
}
