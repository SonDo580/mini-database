package db

import (
	"encoding/binary"
	"errors"
)

type CellType uint8

const (
	TypeI64 CellType = 1
	TypeStr CellType = 2
)

type Cell struct {
	Type CellType
	I64  int64
	Str  []byte
}

func (cell *Cell) Encode(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		return binary.LittleEndian.AppendUint64(toAppend, uint64(cell.I64))
	case TypeStr:
		// serialization format:
		// | length  | data |
		// | 4 bytes | ...  |
		toAppend = binary.LittleEndian.AppendUint32(toAppend, uint32(len(cell.Str)))
		return append(toAppend, cell.Str...)
	}

	panic("unreachable")
}

var ErrExpectMoreData = errors.New("expect more data")

func (cell *Cell) Decode(data []byte) (rest []byte, err error) {
	switch cell.Type {
	case TypeI64:
		if len(data) < 8 {
			return data, ErrExpectMoreData
		}
		cell.I64 = int64(binary.LittleEndian.Uint64(data[:8]))
		return data[8:], nil

	case TypeStr:
		if len(data) < 4 {
			return data, ErrExpectMoreData
		}
		size := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+size {
			return data, ErrExpectMoreData
		}
		cell.Str = data[4 : 4+size]
		return data[4+size:], nil
	}

	panic("unreachable")
}
