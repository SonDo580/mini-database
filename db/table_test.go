package db

import (
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableByPKey(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "link",
		Cols: []Column{
			{Name: "time", Type: TypeI64},
			{Name: "src", Type: TypeStr},
			{Name: "dst", Type: TypeStr},
		},
		PKey: []int{1, 2}, // (src, dst)
	}

	row := Row{
		Cell{Type: TypeI64, I64: 123},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}

	ok, err := db.Select(schema, row)
	assert.True(t, !ok && err == nil)

	updated, err := db.Insert(schema, row)
	assert.True(t, updated && err == nil)

	out := Row{
		Cell{},
		Cell{Type: TypeStr, Str: []byte("a")},
		Cell{Type: TypeStr, Str: []byte("b")},
	}
	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	row[0].I64 = 456
	updated, err = db.Update(schema, row)
	assert.True(t, updated && err == nil)

	ok, err = db.Select(schema, out)
	assert.True(t, ok && err == nil)
	assert.Equal(t, row, out)

	deleted, err := db.Delete(schema, row)
	assert.True(t, deleted && err == nil)

	ok, err = db.Select(schema, out)
	assert.True(t, !ok && err == nil)
}

func parseStmt(t *testing.T, s string) any {
	p := NewParser(s)
	stmt, err := p.parseStmt()
	require.Nil(t, err)
	return stmt
}

func TestSQLByPKey(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	s := "create table links (time int64, src string, dst string, primary key(src, dst));"
	_, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	// create table: duplicate table name
	s = "create table links (any int64, primary key(any));"
	_, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// create table: primary key not in columns
	s = "create table links_1 (time int64, src string, dst string, primary key(extra));"
	_, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// insert: schema mismatch
	s = "insert into links values('123', 'bob', 'alice');"
	r, err := db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// insert: schema mismatch (not enough values)
	s = "insert into links values(123, 'bob');"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// insert: table not found
	s = "insert into xxx values(123, 'bob', 'alice');"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	s = "insert into links values(123, 'bob', 'alice');"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Updated, 1)

	// select: table not found
	s = "select time from xxx where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// select: condition is not primary key
	s = "select time from links where dst = 'alice';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// select: selected columns not found
	s = "select any from links where dst = 'alice';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// select: record not found
	s = "select time from links where dst = 'alice' and src = 'not bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, len(r.Values), 0)

	s = "select time from links where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Values, []Row{{Cell{Type: TypeI64, I64: 123}}})

	// update: table not found
	s = "update xxx set time = 456 where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// update: condition is not primary key
	s = "update links set time = 456 where dst = 'alice';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// update: cannot update primary key
	s = "update links set dst = 'not alice' where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// update: column to update not found
	s = "update links set any = 'any' where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// update: no affected records (key not found)
	s = "update links set time = 456 where dst = 'alice' and src = 'not bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Updated, 0)

	s = "update links set time = 456 where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Updated, 1)

	s = "select time from links where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Values, []Row{{Cell{Type: TypeI64, I64: 456}}})

	// REOPEN
	err = db.Close()
	require.Nil(t, err)
	db = DB{}
	db.KV.log.FileName = ".test_db"
	err = db.Open()
	require.Nil(t, err)

	// delete: table not found
	s = "delete from xxx where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// delete: condition is not primary key
	s = "delete from links where dst = 'alice';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.NotNil(t, err)

	// delete: no affected records (key not found)
	s = "delete from links where dst = 'alice' and src = 'not bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Updated, 0)

	s = "delete from links where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, r.Updated, 1)

	s = "select time from links where dst = 'alice' and src = 'bob';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, len(r.Values), 0)
}

func TestIterByPKey(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	schema := &Schema{
		Table: "t",
		Cols: []Column{
			{Name: "k", Type: TypeI64},
			{Name: "v", Type: TypeI64},
		},
		PKey: []int{0},
	}

	N := int64(10)
	sorted := []int64{} // store inserted values in ascending order
	for i := int64(0); i < N; i += 2 {
		sorted = append(sorted, i)
		row := Row{
			Cell{Type: TypeI64, I64: i}, // k
			Cell{Type: TypeI64, I64: i}, // v
		}
		updated, err := db.Insert(schema, row)
		require.True(t, updated && err == nil)
	}

	for i := int64(-1); i < N+1; i++ {
		row := Row{
			Cell{Type: TypeI64, I64: i}, // k
			Cell{},
		}

		out := []int64{}
		rowIter, err := db.Seek(schema, row)
		for ; err == nil && rowIter.Valid(); err = rowIter.Next() {
			out = append(out, rowIter.Row()[1].I64) // append v
		}
		require.Nil(t, err)

		expected := []int64{}
		for j := i; j < N; j++ {
			if j >= 0 && j%2 == 0 {
				expected = append(expected, j)
			}
		}
		assert.Equal(t, expected, out)
	}

	/* Iterate through all rows in a range and collect values. */
	drainIter := func(req *RangeReq) (out []int64) {
		rowIter, err := db.Range(schema, req)
		for ; err == nil && rowIter.Valid(); err = rowIter.Next() {
			out = append(out, rowIter.Row()[1].I64) // append v
		}
		require.Nil(t, err)
		return
	}

	/* Collect values in a range from the given sorted slice. */
	rangeQuery := func(sorted []int64, start, stop int64, desc bool) (out []int64) {
		for _, v := range sorted {
			if (!desc && start <= v && v <= stop) ||
				(desc && stop <= v && v <= start) {
				out = append(out, v)
			}
		}
		if desc {
			slices.Reverse(out)
		}
		return out
	}

	/* Validate implementation. */
	testReq := func(req *RangeReq, i, j int64, desc bool) {
		out := drainIter(req)
		expected := rangeQuery(sorted, i, j, desc)
		require.Equal(t, expected, out)
	}

	// Closed range
	for i := int64(-1); i < N+1; i++ {
		for j := int64(-1); j < N+1; j++ {
			req := &RangeReq{
				StartCmp: OP_GE,
				StopCmp:  OP_LE,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i, j, false)

			req = &RangeReq{
				StartCmp: OP_LE,
				StopCmp:  OP_GE,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i, j, true)

			req = &RangeReq{
				StartCmp: OP_GT,
				StopCmp:  OP_LT,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i+1, j-1, false)

			req = &RangeReq{
				StartCmp: OP_LT,
				StopCmp:  OP_GT,
				Start:    []Cell{{Type: TypeI64, I64: i}},
				Stop:     []Cell{{Type: TypeI64, I64: j}},
			}
			testReq(req, i-1, j+1, true)
		}
	}

	// Open-ended range
	for i := int64(-1); i < N+1; i++ {
		req := &RangeReq{
			StartCmp: OP_GE,
			StopCmp:  OP_LE,
			Start:    []Cell{{Type: TypeI64, I64: i}},
			Stop:     nil,
		}
		testReq(req, i, N, false)

		req = &RangeReq{
			StartCmp: OP_LE,
			StopCmp:  OP_GE,
			Start:    []Cell{{Type: TypeI64, I64: i}},
			Stop:     nil,
		}
		testReq(req, i, -1, true)
	}
}

func TestTableExpr(t *testing.T) {
	db := DB{}
	db.KV.log.FileName = ".test_db"
	defer os.Remove(db.KV.log.FileName)

	os.Remove(db.KV.log.FileName)
	err := db.Open()
	assert.Nil(t, err)
	defer db.Close()

	s := "create table t (a int64, b int64, c string, d string, primary key (d));"
	_, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)

	s = "insert into t values (1, 2, 'a', 'b');"
	r, err := db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select a * 4 - b, d + c from t where d = 'b';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{makeRow(2, "ba")}, r.Values)

	s = "update t set a = a - b, b = a, c = d + c where d = 'b';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, 1, r.Updated)

	s = "select a, b, c, d from t where d = 'b';"
	r, err = db.ExecStmt(parseStmt(t, s))
	require.Nil(t, err)
	require.Equal(t, []Row{makeRow(-1, 1, "ba", "b")}, r.Values)
}
