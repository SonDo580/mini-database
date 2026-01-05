package db

import (
	"encoding/binary"
	"io"
)

type Entry struct {
	key []byte
	val []byte
}

func (ent *Entry) Encode() []byte {
	// format:
	// 	| key size | val size | key data | val data |
	// 	| 4 bytes  | 4 bytes  |  ...     |  ...     |
	// size is stored as little-endian uint32
	data := make([]byte, 8+len(ent.key)+len(ent.val))
	binary.LittleEndian.PutUint32(data[0:4], uint32(len(ent.key)))
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(ent.val)))
	copy(data[8:], ent.key)
	copy(data[8+len(ent.key):], ent.val)
	return data
}

func (ent *Entry) Decode(r io.Reader) error {
	header := make([]byte, 8)
	if _, err := r.Read(header); err != nil {
		return err
	}

	key_len := binary.LittleEndian.Uint32(header[0:4])
	val_len := binary.LittleEndian.Uint32(header[4:8])
	data := make([]byte, key_len+val_len)
	if _, err := r.Read(data); err != nil {
		return err
	}

	ent.key = data[:key_len]
	ent.val = data[key_len:]

	return nil
}
