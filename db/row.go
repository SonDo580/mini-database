package db

import (
	"errors"
	"slices"
)

type Column struct {
	Name string
	Type CellType
}

type Schema struct {
	Table string // table name
	Cols  []Column
	PKey  []int // indices of primary key columns
}

type Row []Cell

func (schema *Schema) NewRow() Row {
	return make(Row, len(schema.Cols))
}

/* Row as KV:
- primary key as K.
- other columns as V.
*/

func (row Row) EncodeKey(schema *Schema) (key []byte) {
	check(len(row) == len(schema.Cols))

	// - Avoid key conflicts from different tables
	//   -> use table name
	// - Avoid key conflicts from name prefixes
	//   (example: table='abc', key='d' AND table='ab', key='cd')
	//   -> add a separator
	key = append([]byte(schema.Table), 0x00)

	for _, idx := range schema.PKey {
		check(row[idx].Type == schema.Cols[idx].Type)
		key = row[idx].EncodeKey(key)
	}
	return key
}

func (row Row) EncodeVal(schema *Schema) (val []byte) {
	check(len(row) == len(schema.Cols))

	for i, cell := range row {
		if !slices.Contains(schema.PKey, i) {
			check(cell.Type == schema.Cols[i].Type)
			val = cell.EncodeVal(val)
		}
	}
	return val
}

// The key being decoded does not belong to current table
var ErrOutOfRange = errors.New("out of range")

var ErrTrailingGarbage = errors.New("trailing garbage")

func (row Row) DecodeKey(schema *Schema, key []byte) (err error) {
	check(len(row) == len(schema.Cols))

	if len(key) < len(schema.Table)+1 {
		return ErrOutOfRange
	}
	if string(key[:len(schema.Table)+1]) != schema.Table+"\x00" {
		return ErrOutOfRange
	}
	key = key[len(schema.Table)+1:] // skip table name and separator

	for _, idx := range schema.PKey {
		row[idx] = Cell{Type: schema.Cols[idx].Type}
		if key, err = row[idx].DecodeKey(key); err != nil {
			return err
		}
	}

	if len(key) != 0 {
		return ErrTrailingGarbage
	}
	return nil
}

func (row Row) DecodeVal(schema *Schema, val []byte) (err error) {
	check(len(row) == len(schema.Cols))

	for i := range row {
		if slices.Contains(schema.PKey, i) {
			continue
		}
		row[i].Type = schema.Cols[i].Type
		if val, err = row[i].DecodeVal(val); err != nil {
			return err
		}
	}

	if len(val) != 0 {
		return ErrTrailingGarbage
	}
	return nil
}
