package db

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

type Entry struct {
	key     []byte
	val     []byte
	deleted bool
}

/*
Serialization format:
| checksum | key size | val size | deleted | key data | val data |
| 4 bytes  | 4 bytes  | 4 bytes  | 1 byte  |   ...    |   ...    |
*/

func (ent *Entry) Encode() []byte {
	data_len := 13 + len(ent.key)
	if !ent.deleted {
		data_len += len(ent.val)
	}
	data := make([]byte, data_len)

	binary.LittleEndian.PutUint32(data[4:8], uint32(len(ent.key)))
	copy(data[13:], ent.key)

	if ent.deleted {
		data[12] = 1
	} else {
		binary.LittleEndian.PutUint32(data[8:12], uint32(len(ent.val)))
		copy(data[13+len(ent.key):], ent.val)
	}

	checksum := crc32.ChecksumIEEE(data[4:])
	binary.LittleEndian.PutUint32(data[:4], checksum)

	return data
}

var ErrBadSum = errors.New("bad checksum")

func (ent *Entry) Decode(r io.Reader) error {
	header := make([]byte, 13)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	key_len := binary.LittleEndian.Uint32(header[4:8])
	val_len := binary.LittleEndian.Uint32(header[8:12])
	deleted := header[12]

	data := make([]byte, key_len+val_len)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	hash := crc32.NewIEEE()
	hash.Write(header[4:])
	hash.Write(data)
	if hash.Sum32() != binary.LittleEndian.Uint32(header[:4]) {
		return ErrBadSum
	}

	ent.key = data[:key_len]
	if deleted != 0 {
		ent.deleted = true
	} else {
		ent.deleted = false
		ent.val = data[key_len:]
	}

	return nil
}
