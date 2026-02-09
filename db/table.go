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
	schema *Schema
	kvIter *KVIterator
	valid  bool // False if the key being decoded does not belong to current table
	row    Row  // current row (decoded)
}

/* Convert current KV pair into a row. */
func decodeKVIter(schema *Schema, kvIter *KVIterator, row Row) (valid bool, err error) {
	if !kvIter.Valid() {
		return false, nil
	}

	err = row.DecodeKey(schema, kvIter.Key())
	if err == ErrOutOfRange {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err = row.DecodeVal(schema, kvIter.Val()); err != nil {
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
	if err = rowIter.kvIter.Next(); err != nil {
		return err
	}
	rowIter.valid, err = decodeKVIter(rowIter.schema, rowIter.kvIter, rowIter.row)
	return err
}

/* Create a row iterator at the first position >= primary key. */
func (db *DB) Seek(schema *Schema, row Row) (*RowIterator, error) {
	key := row.EncodeKey(schema)
	kvIter, err := db.KV.Seek(key)
	if err != nil {
		return nil, err
	}

	valid, err := decodeKVIter(schema, kvIter, row)
	if err != nil {
		return nil, err
	}

	rowIter := &RowIterator{
		schema: schema,
		kvIter: kvIter,
		valid:  valid,
		row:    row,
	}
	return rowIter, nil
}

type SQLResult struct {
	// SELECT returns Header and Values
	Headers []string
	Values  []Row

	// Other statements return Updated
	Updated int // number of affected rows
}

func (db *DB) ExecStmt(stmt any) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmtCreateTable:
		err = db.execCreateTable(ptr)
	case *StmtSelect:
		r.Headers = ptr.cols
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

func (db *DB) execSelect(stmt *StmtSelect) ([]Row, error) {
	schema, err := db.GetSchema(stmt.table)
	if err != nil {
		return nil, err
	}

	indices, err := lookupColumns(schema.Cols, stmt.cols)
	if err != nil {
		return nil, err
	}

	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return nil, err
	}

	ok, err := db.Select(&schema, row)
	if err != nil || !ok {
		return nil, err
	}

	row = subsetRow(row, indices)
	return []Row{row}, nil
}

/* Create a row with just primary key filled. */
func makePKey(schema *Schema, pkey []NamedCell) (Row, error) {
	if len(schema.PKey) != len(pkey) {
		return nil, errors.New("not primary key")
	}

	row := schema.NewRow()
	for _, idxInSchema := range schema.PKey {
		schemaCol := schema.Cols[idxInSchema]
		idxInStmt := slices.IndexFunc(pkey, func(key NamedCell) bool {
			return key.column == schemaCol.Name && key.value.Type == schemaCol.Type
		})
		if idxInStmt == -1 {
			return nil, errors.New("not primary key")
		}
		row[idxInSchema] = pkey[idxInStmt].value
	}

	return row, nil
}

func subsetRow(row Row, indices []int) (out Row) {
	for _, idx := range indices {
		out = append(out, row[idx])
	}
	return
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

	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return 0, err
	}

	if err = fillNonPKey(&schema, stmt.value, row); err != nil {
		return 0, err
	}

	updated, err := db.Update(&schema, row)
	if err != nil {
		return 0, err
	}
	if updated {
		count++
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

	row, err := makePKey(&schema, stmt.keys)
	if err != nil {
		return 0, err
	}

	deleted, err := db.Delete(&schema, row)
	if err != nil {
		return 0, err
	}
	if deleted {
		count++
	}
	return count, nil
}
