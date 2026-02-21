package db

import (
	"encoding/json"
	"errors"
	"slices"
)

type DB struct {
	KV     KV
	tables map[string]Schema
}

func (db *DB) Open() error {
	db.tables = map[string]Schema{}
	return db.KV.Open()
}

func (db *DB) Close() error {
	return db.KV.Close()
}

func (db *DB) Select(schema *Schema, row Row) (ok bool, err error) {
	key := row.EncodeKey(schema)
	val, ok, err := db.KV.Get(key)
	if err != nil || !ok {
		return
	}
	err = row.DecodeVal(schema, val)
	return
}

func (db *DB) Insert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeInsert)
}

func (db *DB) Update(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpdate)
}

func (db *DB) Upsert(schema *Schema, row Row) (updated bool, err error) {
	key := row.EncodeKey(schema)
	val := row.EncodeVal(schema)
	return db.KV.SetEx(key, val, ModeUpsert)
}

func (db *DB) Delete(schema *Schema, row Row) (deleted bool, err error) {
	key := row.EncodeKey(schema)
	return db.KV.Del(key)
}

type RowIterator struct {
	schema       *Schema
	rangedKVIter *RangedKVIter
	valid        bool // False if the key being decoded does not belong to current table
	row          Row  // current row (decoded)
}

/* Convert current KV pair into a row. */
func decodeKVIter(schema *Schema, rangedKVIter *RangedKVIter, row Row) (valid bool, err error) {
	if !rangedKVIter.Valid() {
		return false, nil
	}
	if err = row.DecodeKey(schema, rangedKVIter.Key()); err != nil {
		check(err != ErrOutOfRange)
		return false, err
	}
	if err = row.DecodeVal(schema, rangedKVIter.Val()); err != nil {
		return false, err
	}
	return true, nil
}

/* True if still in table key range. */
func (rowIter *RowIterator) Valid() bool {
	return rowIter.valid
}

/* Current row */
func (rowIter *RowIterator) Row() Row {
	check(rowIter.valid)
	return rowIter.row
}

/* Move to the next row. */
func (rowIter *RowIterator) Next() (err error) {
	if err = rowIter.rangedKVIter.Next(); err != nil {
		return err
	}
	rowIter.valid, err = decodeKVIter(rowIter.schema, rowIter.rangedKVIter, rowIter.row)
	return err
}

/* Create a row iterator for the range [start, +inf). */
func (db *DB) Seek(schema *Schema, row Row) (*RowIterator, error) {
	start := make([]Cell, len(schema.PKey))
	for i, idx := range schema.PKey {
		check(row[idx].Type == schema.Cols[idx].Type)
		start[i] = row[idx]
	}

	return db.Range(schema, &RangeReq{
		StartCmp: OP_GE,
		StopCmp:  OP_LE,
		Start:    start,
		Stop:     nil,
	})
}

type RangeReq struct {
	StartCmp ExprOp
	StopCmp  ExprOp
	Start    []Cell
	Stop     []Cell
}

/* Create a row iterator for the specified range. */
func (db *DB) Range(schema *Schema, req *RangeReq) (*RowIterator, error) {
	check(isDescending(req.StartCmp) != isDescending(req.StopCmp))

	start := EncodeKeyPrefix(schema, req.Start, suffixPositive(req.StartCmp))
	stop := EncodeKeyPrefix(schema, req.Stop, suffixPositive(req.StopCmp))
	desc := isDescending(req.StartCmp)
	rangedKVIter, err := db.KV.Range(start, stop, desc)
	if err != nil {
		return nil, err
	}

	row := schema.NewRow()
	valid, err := decodeKVIter(schema, rangedKVIter, row)
	if err != nil {
		return nil, err
	}

	return &RowIterator{
		schema:       schema,
		rangedKVIter: rangedKVIter,
		valid:        valid,
		row:          row,
	}, nil
}

/* If True, add +inf as suffix. Otherwise add -inf. */
func suffixPositive(cmp ExprOp) bool {
	return cmp == OP_LE || cmp == OP_GT
}

/* Determine scan direction. */
func isDescending(startCmp ExprOp) bool {
	return startCmp == OP_LE || startCmp == OP_LT
}

// SELECT returns Header and Values.
// Other statements return Updated.
type SQLResult struct {
	Headers []string
	Values  []Row
	Updated int // number of affected rows
}

func (db *DB) ExecStmt(stmt any) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreateTable:
		err = db.execCreateTable(ptr)
	case *StmtSelect:
		r.Headers = exprs2header(ptr.cols)
		r.Values, err = db.execSelect(ptr)
	case *StmtInsert:
		r.Updated, err = db.execInsert(ptr)
	case *StmtUpdate:
		r.Updated, err = db.execUpdate(ptr)
	case *StmtDelete:
		r.Updated, err = db.execDelete(ptr)
	default:
		panic("unreachable")
	}
	return
}

func (db *DB) execCreateTable(stmt *StmtCreateTable) (err error) {
	if _, err := db.GetSchema(stmt.table); err == nil {
		return errors.New("duplicate table name")
	}

	schema := Schema{
		Table: stmt.table,
		Cols:  stmt.cols,
	}

	if schema.PKey, err = lookupColumns(stmt.cols, stmt.pkey); err != nil {
		return err
	}

	val, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	if _, err = db.KV.Set([]byte("@schema_"+stmt.table), val); err != nil {
		return err
	}

	db.tables[stmt.table] = schema
	return nil
}

func (db *DB) GetSchema(table string) (Schema, error) {
	schema, ok := db.tables[table]
	if !ok {
		val, ok, err := db.KV.Get([]byte("@schema_" + table))
		if err != nil {
			return Schema{}, err
		}
		if !ok {
			return Schema{}, errors.New("table not found")
		}

		if err = json.Unmarshal(val, &schema); err != nil {
			return Schema{}, err
		}

		db.tables[table] = schema
	}

	return schema, nil
}

/* Lookup column indices in table schema */
func lookupColumns(cols []Column, names []string) (indices []int, err error) {
	for _, name := range names {
		idx := slices.IndexFunc(cols, func(col Column) bool {
			return col.Name == name
		})
		if idx == -1 {
			return nil, errors.New("column not found")
		}
		indices = append(indices, idx)
	}
	return
}

/* Produce row iterator on a table from the WHERE condition. */
func (db *DB) execCond(schema *Schema, cond interface{}) (*RowIterator, error) {
	req, err := makeRange(schema, cond)
	if err != nil {
		return nil, err
	}
	return db.Range(schema, req)
}

/* Produce range query from the WHERE condition. */
func makeRange(schema *Schema, cond interface{}) (*RangeReq, error) {
	if keys, ok := matchAllEq(cond, nil); ok {
		if pkey, ok := extractPKey(schema, keys); ok {
			return &RangeReq{
				StartCmp: OP_GE,
				StopCmp:  OP_LE,
				Start:    pkey,
				Stop:     pkey,
			}, nil
		}
	}

	if req, ok := matchRange(schema, cond); ok {
		return req, nil
	}

	return nil, errors.New("unimplemented WHERE")
}

/* Example match: a = 1 AND b = 'b' AND 1 = c ... */
func matchAllEq(cond interface{}, out []NamedCell) ([]NamedCell, bool) {
	expr, ok := cond.(*ExprBinOp)
	if !ok {
		return nil, false
	}

	if expr.op == OP_EQ {
		left, right := expr.left, expr.right
		column, ok := left.(string)
		if !ok {
			left, right = right, left
			column, ok = left.(string)
		}
		if !ok {
			return nil, false
		}

		cell, ok := right.(*Cell)
		if !ok {
			return nil, false
		}
		return append(out, NamedCell{column: column, value: *cell}), true
	}

	if expr.op == OP_AND {
		if out, ok = matchAllEq(expr.left, out); !ok {
			return nil, false
		}
		if out, ok = matchAllEq(expr.right, out); !ok {
			return nil, false
		}
		return out, true
	}

	return nil, false
}

/*
Extract queried primary key value from condition.
Constraints: must match all components, don't support partial match.
*/
func extractPKey(schema *Schema, pkey []NamedCell) (cells []Cell, ok bool) {
	if len(schema.PKey) != len(pkey) {
		return nil, false
	}

	for _, idx := range schema.PKey {
		col := schema.Cols[idx]
		i := slices.IndexFunc(pkey, func(key NamedCell) bool {
			return key.column == col.Name && key.value.Type == col.Type
		})
		if i < 0 {
			return nil, false
		}
		cells = append(cells, pkey[i].value)
	}

	return cells, true
}

/*
Constraints:
  - Only support single range (To support OR, we would need multiple ranges).
  - Only allowed inequality operators.
  - If the condition is a comparison expression, it must define a half-open range.
    Direction depends on the operator.
  - If the condition is an AND expression, it must define a double-bounded range.
    One side defines the start, the other side defines the stop.
    The comparisons must be in opposite direction, don't support range merging.
    Always convert to an ascending range.
  - In each comparison expression, 1 side must be column(s), the other side must be value(s).
    The column(s) must be a prefix of the primary key.
    The prefix must be in correct order, don't support re-order.

Example match:
. a >= 1 AND b < 2
. (a, b) >= (1, 2)
. (a, b) >= (1, 2) AND (a, b, c) < (2, 3, 4)
. ...
*/
func matchRange(schema *Schema, cond interface{}) (*RangeReq, bool) {
	binop, ok := cond.(*ExprBinOp)
	if ok && binop.op == OP_AND {
		// Double-bounded range
		startCmp, startCols, startCells, ok := matchCmp(cond)
		if !ok || !isPKeyPrefix(schema, startCols, startCells) {
			return nil, false
		}

		stopCmp, stopCols, stopCells, ok := matchCmp(cond)
		if !ok || !isPKeyPrefix(schema, stopCols, stopCells) {
			return nil, false
		}

		// Comparisons must have opposite direction
		if isDescending(startCmp) == isDescending(stopCmp) {
			return nil, false
		}

		// Make the range ascending
		if isDescending(startCmp) {
			startCmp, stopCmp = stopCmp, startCmp
			startCells, stopCells = stopCells, startCells
		}

		return &RangeReq{
			StartCmp: startCmp,
			StopCmp:  stopCmp,
			Start:    startCells,
			Stop:     stopCells,
		}, true
	} else if ok {
		// Half-open range
		startCmp, startCols, startCells, ok := matchCmp(cond)
		if !ok || !isPKeyPrefix(schema, startCols, startCells) {
			return nil, false
		}

		stopCmp := OP_LE
		if isDescending(startCmp) {
			stopCmp = OP_GE
		}

		return &RangeReq{
			StartCmp: startCmp,
			StopCmp:  stopCmp,
			Start:    startCells,
			Stop:     nil,
		}, true
	}

	return nil, false
}

/* Match a list of columns compared against a list of values. */
func matchCmp(cond interface{}) (ExprOp, []string, []Cell, bool) {
	binop, ok := cond.(*ExprBinOp)
	if !ok {
		return 0, nil, nil, false
	}

	// Only allow inequality operators
	op := binop.op
	switch op {
	case OP_LE, OP_GE, OP_LT, OP_GT:
		break
	default:
		return 0, nil, nil, false
	}

	// Identify which side contains column names
	left, right := binop.left, binop.right
	names, ok := asNameList(left)
	if !ok {
		left, right = right, left
		names, ok = asNameList(left)

		// Swap op direction
		switch op {
		case OP_GE:
			op = OP_LE
		case OP_LE:
			op = OP_GE
		case OP_GT:
			op = OP_LT
		case OP_LT:
			op = OP_GT
		}
	}
	if !ok {
		return 0, nil, nil, false
	}

	// The other side must be values
	cells, ok := asCellList(right)
	if !ok {
		return 0, nil, nil, false
	}

	return op, names, cells, true
}

func asNameList(expr interface{}) (out []string, ok bool) {
	switch e := expr.(type) {
	case string:
		return []string{e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			str, ok := kid.(string)
			if !ok {
				return nil, false
			}
			out = append(out, str)
		}
		return out, true
	}
	return nil, false
}

func asCellList(expr interface{}) (out []Cell, ok bool) {
	switch e := expr.(type) {
	case *Cell:
		return []Cell{*e}, true
	case *ExprTuple:
		for _, kid := range e.kids {
			cell, ok := kid.(*Cell)
			if !ok {
				return nil, false
			}
			out = append(out, *cell)
		}
		return out, true
	}
	return nil, false
}

func isPKeyPrefix(schema *Schema, cols []string, cells []Cell) bool {
	if len(cols) != len(cells) || len(cols) > len(schema.Cols) {
		return false
	}
	for i := range cols {
		col := schema.Cols[schema.PKey[i]]
		if col.Name != cols[i] || col.Type != cells[i].Type {
			return false
		}
	}
	return true
}

func (db *DB) execSelect(stmt *StmtSelect) (output []Row, err error) {
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	rowIter, err := db.execCond(&schema, stmt.cond)
	if err != nil {
		return nil, err
	}

	for ; err == nil && rowIter.Valid(); err = rowIter.Next() {
		row := rowIter.Row()
		computed := make(Row, len(stmt.cols))
		for i, expr := range stmt.cols {
			cell, err := evalExpr(&schema, row, expr)
			if err != nil {
				return nil, err
			}
			computed[i] = *cell
		}
		output = append(output, computed)
	}

	if err != nil {
		return nil, err
	}
	return output, nil
}

func (db *DB) execInsert(stmt *StmtInsert) (count int, err error) {
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	if len(schema.Cols) != len(stmt.value) {
		return 0, errors.New("schema mismatch")
	}
	for i := range schema.Cols {
		if schema.Cols[i].Type != stmt.value[i].Type {
			return 0, errors.New("schema mismatch")
		}
	}

	inserted, err := db.Insert(&schema, stmt.value)
	if err != nil {
		return 0, err
	}
	if inserted {
		count++
	}
	return count, nil
}

func (db *DB) execUpdate(stmt *StmtUpdate) (count int, err error) {
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	rowIter, err := db.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	for ; err == nil && rowIter.Valid(); err = rowIter.Next() {
		row := rowIter.Row()
		updates := make([]NamedCell, len(stmt.value))
		for i, assign := range stmt.value {
			cell, err := evalExpr(&schema, row, assign.expr)
			if err != nil {
				return 0, err
			}
			updates[i] = NamedCell{column: assign.column, value: *cell}
		}

		if err = fillNonPKey(&schema, updates, row); err != nil {
			return 0, err
		}

		updated, err := db.Update(&schema, row)
		if err != nil {
			return 0, err
		}
		if updated {
			count++
		}
	}

	if err != nil {
		return 0, err
	}
	return count, nil
}

/* Fill cells with updated values. Don't allow updating primary key. */
func fillNonPKey(schema *Schema, updates []NamedCell, out Row) error {
	for _, cell := range updates {
		idxInSchema := slices.IndexFunc(schema.Cols, func(col Column) bool {
			return col.Name == cell.column && col.Type == cell.value.Type
		})
		if idxInSchema == -1 {
			return errors.New("column to update not found")
		}
		if slices.Contains(schema.PKey, idxInSchema) {
			return errors.New("cannot update primary key")
		}

		out[idxInSchema] = cell.value
	}
	return nil
}

func (db *DB) execDelete(stmt *StmtDelete) (count int, err error) {
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return 0, err
	}

	rowIter, err := db.execCond(&schema, stmt.cond)
	if err != nil {
		return 0, err
	}

	for ; err == nil && rowIter.Valid(); rowIter.Next() {
		row := rowIter.Row()
		deleted, err := db.Delete(&schema, row)
		if err != nil {
			return 0, err
		}
		if deleted {
			count++
		}
	}

	if err != nil {
		return 0, err
	}
	return count, nil
}
