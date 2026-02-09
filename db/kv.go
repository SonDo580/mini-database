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
func (kvIter *KVIterator) Valid() bool {
	return 0 <= kvIter.pos && kvIter.pos < len(kvIter.keys)
}

/* Key of current entry */
func (kvIter *KVIterator) Key() []byte {
	return kvIter.keys[kvIter.pos]
}

/* Value of current entry */
func (kvIter *KVIterator) Val() []byte {
	return kvIter.vals[kvIter.pos]
}

/* Move to the next entry */
func (kvIter *KVIterator) Next() error {
	if kvIter.pos < len(kvIter.keys) {
		kvIter.pos++
	}
	return nil
}

/* Move to the previous entry */
func (kvIter *KVIterator) Prev() error {
	if kvIter.pos >= 0 {
		kvIter.pos--
	}
	return nil
}

type RangedKVIter struct {
	kvIter KVIterator
	stop   []byte // Stop key
	desc   bool   // True if iterate in descending order
}

/* True if still in key range and haven't gone pass stop key. */
func (rangedKVIter *RangedKVIter) Valid() bool {
	if !rangedKVIter.kvIter.Valid() {
		return false
	}

	r := bytes.Compare(rangedKVIter.kvIter.Key(), rangedKVIter.stop)
	if rangedKVIter.desc && r < 0 {
		return false
	} else if !rangedKVIter.desc && r > 0 {
		return false
	}
	return true
}

/* Key of current entry */
func (rangedKVIter *RangedKVIter) Key() []byte {
	check(rangedKVIter.Valid())
	return rangedKVIter.kvIter.Key()
}

/* Value of current entry */
func (rangedKVIter *RangedKVIter) Val() []byte {
	check(rangedKVIter.Valid())
	return rangedKVIter.kvIter.Val()
}

/* Move to the next entry */
func (rangedKVIter *RangedKVIter) Next() error {
	if !rangedKVIter.Valid() {
		return nil
	}
	if rangedKVIter.desc {
		return rangedKVIter.kvIter.Prev()
	} else {
		return rangedKVIter.kvIter.Next()
	}
}

/* Create a ranged KV iterator from start to stop or stop to start .*/
func (kv *KV) Range(start, stop []byte, desc bool) (*RangedKVIter, error) {
	kvIter, err := kv.Seek(start)
	if err != nil {
		return nil, err
	}

	// kvIter points at the first key >= start, or after the last key
	// for descending range we need that last key <= start
	if desc && (!kvIter.Valid() || bytes.Compare(kvIter.Key(), start) > 0) {
		if err = kvIter.Prev(); err != nil {
			return nil, err
		}
	}

	rangedKVIter := &RangedKVIter{
		kvIter: *kvIter,
		stop:   stop,
		desc:   desc,
	}
	return rangedKVIter, nil
}
