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

Avoid key conflicts:
- Avoid key conflicts from different tables -> use table name
- Avoid key conflicts from name prefixes -> add a separator
  (example: table='abc', key='d' AND table='ab', key='cd')
*/

/* Prefix comparison:
Problem:
- Primary key can consist of multiple columns.
  A query may only use a prefix.
  . (a, b, c) <= (1, 2, 3) -> use full key
  . (a, b) <= (1, 2) -> equivalent to (a, b, c) <= (1, 2, +inf)
  . (a, b) < (1, 2) -> equivalent to (a, b, c) <= (1, 2, -inf)
- If we only encode the prefix,
  it will always be (lexicographically) smaller than a full key
  . (a, b) <= (1, 2) exclude all (1, 2, x) keys

Solution: Include infinity in KV key encoding
- Empty string (the smallest string) means -inf.
  This requires serialized entry to never be "" -> already true.
- "\xff" (0xff, the max byte) means +inf.
  This requires serialized entry to never start with "\xff".
  -> prepend 1 byte to each column (use column type).
- Avoid conflict between (a, b) and (a, b, -inf)
  -> append a trailing 0x00 (> "", the -inf)
*/

func (row Row) EncodeKey(schema *Schema) (key []byte) {
	check(len(row) == len(schema.Cols))
	key = append([]byte(schema.Table), 0x00)

	for _, idx := range schema.PKey {
		cell := row[idx]
		check(cell.Type == schema.Cols[idx].Type)
		key = append(key, byte(cell.Type)) // avoid 0xff
		key = cell.EncodeKey(key)
	}

	// avoid conflict between (a, b) and (a, b, -inf)
	return append(key, 0x00) // > -inf
}

func EncodeKeyPrefix(schema *Schema, prefix []Cell, positive bool) []byte {
	key := append([]byte(schema.Table), 0x00)
	for _, cell := range prefix {
		key = append(key, byte(cell.Type)) // avoid 0xff
		key = cell.EncodeKey(key)
	}

	// next entry is -inf by default
	if positive {
		key = append(key, 0xff) // +inf
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

var ErrBadKey = errors.New("bad key")
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
		if len(key) == 0 || key[0] != byte(row[idx].Type) {
			return ErrBadKey
		}

		key = key[1:]
		if key, err = row[idx].DecodeKey(key); err != nil {
			return err
		}
	}

	if len(key) != 1 || key[0] != 0x00 {
		return ErrBadKey
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
