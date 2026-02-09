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

func (cell *Cell) EncodeVal(toAppend []byte) []byte {
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

func (cell *Cell) DecodeVal(data []byte) (rest []byte, err error) {
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

/* Order preserving serialization (string compare reflects sort order)

1. int64
- For uint64, we should use big-endian encoding,
  since string compare starts at byte 0.
- We have to remap int64 to uint64:
  . non-negative numbers [0, 2^63 - 1] -> same in uint64
  . negative numbers [-2^63, -1] -> [2^63, 2^64 - 1] in uint64
- The negative range is mapped to the larger halve in uint64
  -> flip the MSB to swap the 2 halves.
  . to flip the MSB of x: x ^ (1 << 63)

2. string
- Length-prefixed format no longer works.
- Use a delimiter (0 byte) to mark the end (null-terminated string).
- If string data contains 0 bytes, they must be escaped.
  The escaped byte itself must also be escaped.
  . 0x00 <=> 0x01 0x01
  . 0x01 <=> 0x01 0x02
- If a string is a prefix of the other, comparison will reach the 0 byte
  and decide that the shorter string is smaller.
*/

func encodeStrKey(toAppend []byte, input []byte) []byte {
	for _, ch := range input {
		if ch == 0x00 || ch == 0x01 {
			toAppend = append(toAppend, 0x01, ch+1)
		} else {
			toAppend = append(toAppend, ch)
		}
	}
	return append(toAppend, 0x00)
}

func decodeStrKey(data []byte) (out []byte, rest []byte, err error) {
	if len(data) == 0 {
		return nil, data, errors.New("expect more data")
	}

	var escape bool
	for i, ch := range data {
		if ch == 0x00 {
			return out, data[i+1:], nil
		}

		if escape {
			if ch != 0x01 && ch != 0x02 {
				return nil, data, errors.New("bad escape")
			}
			out = append(out, ch-1)
			escape = false
			continue
		}

		if ch == 0x01 {
			escape = true
		} else {
			out = append(out, ch)
		}
	}

	return nil, data, errors.New("string is not terminated")
}

func (cell *Cell) EncodeKey(toAppend []byte) []byte {
	switch cell.Type {
	case TypeI64:
		return binary.BigEndian.AppendUint64(toAppend, uint64(cell.I64)^(1<<63))
	case TypeStr:
		return encodeStrKey(toAppend, cell.Str)
	default:
		panic("unreachable")
	}
}

func (cell *Cell) DecodeKey(data []byte) (rest []byte, err error) {
	switch cell.Type {
	case TypeI64:
		if len(data) < 8 {
			return data, errors.New("expect more data")
		}
		cell.I64 = int64(binary.BigEndian.Uint64(data[0:8]) ^ (1 << 63))
		return data[8:], nil
	case TypeStr:
		cell.Str, rest, err = decodeStrKey(data)
		return rest, err
	default:
		panic("unreachable")
	}
}
