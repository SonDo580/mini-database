package db

import (
	"encoding/binary"
	"io"
)

type Entry struct {
	key     []byte
	val     []byte
	deleted bool
}

func (ent *Entry) Encode() []byte {
	// . format:
	// | key size | val size | deleted | key data | val data |
	// | 4 bytes  | 4 bytes  | 1 byte  |   ...    |   ...    |
	// . size is stored as little-endian uint32
	data_len := 9 + len(ent.key)
	if !ent.deleted {
		data_len += len(ent.val)
	}
	data := make([]byte, data_len)

	binary.LittleEndian.PutUint32(data[0:4], uint32(len(ent.key)))
	copy(data[9:], ent.key)

	if ent.deleted {
		data[8] = 1
	} else {
		binary.LittleEndian.PutUint32(data[4:8], uint32(len(ent.val)))
		copy(data[9+len(ent.key):], ent.val)
	}

	return data
}

func (ent *Entry) Decode(r io.Reader) error {
	header := make([]byte, 9)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	key_len := binary.LittleEndian.Uint32(header[0:4])
	val_len := binary.LittleEndian.Uint32(header[4:8])
	deleted := header[8]

	data := make([]byte, key_len+val_len)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
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
