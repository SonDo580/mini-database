package db

import (
	"bytes"
	"slices"
)

type KV struct {
	log Log
	mem SortedArray
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}

	entries := []Entry{}
	for {
		ent := Entry{}
		eof, err := kv.log.Read(&ent)
		if err != nil {
			return err
		} else if eof {
			break
		}
		entries = append(entries, ent)
	}

	// Use stable sort to preserve logged order for the same key
	slices.SortStableFunc(entries, func(a, b Entry) int {
		return bytes.Compare(a.key, b.key)
	})

	kv.mem.Clear()
	for _, ent := range entries {
		// Only use the last entry of each key
		// (Note that all entries for a key are adjacent due to sorting)
		n := kv.mem.Size()
		if n > 0 && bytes.Equal(kv.mem.Key(n-1), ent.key) {
			kv.mem.Pop()
		}

		// Don't add deleted entries
		if !ent.deleted {
			kv.mem.Push(ent.key, ent.val)
		}
	}
	return nil
}

func (kv *KV) Close() error {
	return kv.log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	return kv.mem.Get(key)
}

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	existing_val, exists, err := kv.Get(key)
	if err != nil {
		return false, err
	}

	var needUpdate bool
	switch mode {
	case ModeUpsert:
		needUpdate = !exists || !bytes.Equal(existing_val, val)
	case ModeInsert:
		needUpdate = !exists
	case ModeUpdate:
		needUpdate = exists && !bytes.Equal(existing_val, val)
	default:
		panic("unreachable")
	}

	if needUpdate {
		if err = kv.log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}
		updated, err = kv.mem.Set(key, val)
		check(err == nil)
	}
	return updated, nil
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	if _, exists, err := kv.Get(key); err != nil || !exists {
		return false, err
	}
	if err = kv.log.Write(&Entry{key: key, deleted: true}); err != nil {
		return false, nil
	}
	deleted, err = kv.mem.Del(key)
	check(err == nil)
	return deleted, err
}

func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	return kv.mem.Seek(key)
}

type RangedKVIter struct {
	kvIter SortedKVIter
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
		kvIter: kvIter,
		stop:   stop,
		desc:   desc,
	}
	return rangedKVIter, nil
}
