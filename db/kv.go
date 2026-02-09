package db

import (
	"bytes"
	"slices"
)

// Use arrays to support sorting and range queries
// TODO: Use LSM tree
type KV struct {
	log  Log
	keys [][]byte
	vals [][]byte
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}

	kv.keys = [][]byte{}
	kv.vals = [][]byte{}
	for {
		ent := Entry{}
		eof, err := kv.log.Read(&ent)
		if err != nil {
			return err
		}
		if eof {
			break
		}

		key := ent.key
		val := ent.val
		idx, exists := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
		if ent.deleted {
			kv.keys = slices.Delete(kv.keys, idx, idx+1)
			kv.vals = slices.Delete(kv.vals, idx, idx+1)
		} else if exists {
			kv.vals[idx] = val
		} else {
			kv.keys = slices.Insert(kv.keys, idx, key)
			kv.vals = slices.Insert(kv.vals, idx, val)
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.keys, key, bytes.Compare); ok {
		return kv.vals[idx], true, nil
	}
	return nil, false, nil
}

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	idx, exists := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
	var needUpdate bool
	switch mode {
	case ModeUpsert:
		needUpdate = !exists || !bytes.Equal(kv.vals[idx], val)
	case ModeInsert:
		needUpdate = !exists
	case ModeUpdate:
		needUpdate = exists && !bytes.Equal(kv.vals[idx], val)
	default:
		panic("unreachable")
	}

	if needUpdate {
		if err = kv.log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}

		if exists {
			kv.vals[idx] = val
		} else {
			kv.keys = slices.Insert(kv.keys, idx, key)
			kv.vals = slices.Insert(kv.vals, idx, val)
		}
		updated = true
	}
	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	idx, exists := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
	if exists {
		if err = kv.log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, nil
		}

		kv.keys = slices.Delete(kv.keys, idx, idx+1)
		kv.vals = slices.Delete(kv.vals, idx, idx+1)
		deleted = true
	}
	return
}

type KVIterator struct {
	keys [][]byte // reference kv.keys
	vals [][]byte // reference kv.vals
	pos  int      // current position
}

/* Point to the first key >= key */
func (kv *KV) Seek(key []byte) (*KVIterator, error) {
	pos, _ := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
	return &KVIterator{keys: kv.keys, vals: kv.vals, pos: pos}, nil
}

/* True if still in key range */
func (iter *KVIterator) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

/* Key of current element */
func (iter *KVIterator) Key() []byte {
	return iter.keys[iter.pos]
}

/* Value of current element */
func (iter *KVIterator) Val() []byte {
	return iter.vals[iter.pos]
}

/* Move to the next element */
func (iter *KVIterator) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos++
	}
	return nil
}

/* Move to the previous element */
func (iter *KVIterator) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}
	return nil
}
