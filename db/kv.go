package db

import (
	"bytes"
	"os"
	"path"
	"slices"
)

type KV struct {
	log          Log
	mem          SortedArray
	main         SortedFile
	MultiClosers // struct embedding
}

func (kv *KV) Open() (err error) {
	if err = kv.openAll(); err != nil {
		_ = kv.Close()
	}
	return err
}

func (kv *KV) openAll() error {
	if err := kv.openLog(); err != nil {
		return err
	}
	return kv.openSSTable()
}

func (kv *KV) openLog() error {
	if err := kv.log.Open(); err != nil {
		return err
	}
	kv.MultiClosers = append(kv.MultiClosers, &kv.log)

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
		kv.mem.Push(ent.key, ent.val, ent.deleted)
	}
	return nil
}

func (kv *KV) openSSTable() error {
	if kv.main.FileName != "" {
		if err := kv.main.Open(); err != nil {
			return err
		}
		kv.MultiClosers = append(kv.MultiClosers, &kv.main)
	}
	return nil
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	iter, err := kv.Seek(key)
	ok = err == nil && iter.Valid() && bytes.Equal(iter.Key(), key)
	if ok {
		val = iter.Val()
	}
	return val, ok, err
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
		return false, err
	}
	deleted, err = kv.mem.Del(key)
	check(err == nil)
	return deleted, err
}

func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	m := MergedSortedKV{&kv.mem, &kv.main}
	iter, err := m.Seek(key)
	if err != nil {
		return nil, err
	}
	return filterDeleted(iter)
}

/* Return a SortedKVIter that skip deleted keys. */
func filterDeleted(iter SortedKVIter) (SortedKVIter, error) {
	for iter.Valid() && iter.Deleted() {
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	return NoDeletedIter{iter}, nil
}

type NoDeletedIter struct {
	SortedKVIter // struct embedding
}

func (iter NoDeletedIter) Next() (err error) {
	err = iter.SortedKVIter.Next()
	for err == nil && iter.Valid() && iter.Deleted() {
		err = iter.SortedKVIter.Next()
	}
	return err
}

func (iter NoDeletedIter) Prev() (err error) {
	err = iter.SortedKVIter.Prev()
	for err == nil && iter.Valid() && iter.Deleted() {
		err = iter.SortedKVIter.Prev()
	}
	return err
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

/* Merge MemTable (mirroring the log) and SSTable to produce a new SSTable. */
func (kv *KV) Compact() error {
	// Merge MemTable and SSTable, output to a temporary file
	check(kv.main.FileName != "")
	fp, err := os.CreateTemp(path.Dir(kv.main.FileName), "tmp_sstable")
	if err != nil {
		return err
	}
	filename := fp.Name()
	_ = fp.Close()
	defer os.Remove(filename)

	file := SortedFile{FileName: filename}
	m := MergedSortedKV{&kv.mem, &kv.main}
	if err := file.CreateFromSorted(m); err != nil {
		return err
	}

	// Replace the original SSTable (atomic operation)
	if err := renameSync(file.FileName, kv.main.FileName); err != nil {
		_ = file.Close()
		return err
	}
	file.FileName = kv.main.FileName
	_ = kv.main.Close()
	kv.main = file

	// Drop the MemTable and the log
	kv.mem.Clear()
	return kv.log.Truncate()
}
