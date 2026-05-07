package db

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"slices"
)

type KVOptions struct {
	Dirpath string // directory to store logs, metadata, SSTables

	// LSM-tree config
	LogThreshold int     // max key count in log (convert to SSTable when reached)
	GrowthFactor float32 // size (key count) ratio between 2 adjacent levels
}

type KV struct {
	Options KVOptions

	// metadata
	meta    KVMetaStore
	version uint64

	// data
	log  Log
	mem  SortedArray
	main []SortedFile

	MultiClosers
}

func (kv *KV) Open() (err error) {
	if err = kv.openAll(); err != nil {
		_ = kv.Close()
	}
	return err
}

func (kv *KV) openAll() error {
	// Create directory if not exist
	// (owner can read, write, execute; group and other can read and execute)
	err := os.Mkdir(kv.Options.Dirpath, 0o755)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}

	if err := kv.openMeta(); err != nil {
		return err
	}
	if err := kv.openLog(); err != nil {
		return err
	}
	return kv.openSSTable()
}

func (kv *KV) openMeta() error {
	kv.meta.slots[0].FileName = path.Join(kv.Options.Dirpath, "meta0")
	kv.meta.slots[1].FileName = path.Join(kv.Options.Dirpath, "meta1")
	if err := kv.meta.Open(); err != nil {
		return err
	}
	kv.MultiClosers = append(kv.MultiClosers, &kv.meta)
	kv.version = kv.meta.Get().Version
	return nil
}

func (kv *KV) openLog() error {
	kv.log.FileName = path.Join(kv.Options.Dirpath, "kv_log")
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
	meta := kv.meta.Get()
	kv.version = meta.Version
	kv.main = kv.main[:0] // clear
	for _, sstable := range meta.SSTables {
		filename := path.Join(kv.Options.Dirpath, sstable)
		file := SortedFile{FileName: filename}
		if err := file.Open(); err != nil {
			return err
		}
		kv.MultiClosers = append(kv.MultiClosers, &file)
		kv.main = append(kv.main, file)
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
	m := MergedSortedKV{&kv.mem}
	for i := range kv.main {
		m = append(m, &kv.main[i])
	}

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

func (kv *KV) Compact() error {
	if kv.mem.Size() >= kv.Options.LogThreshold {
		if err := kv.compactLog(); err != nil {
			return err
		}
	}

	for i := 0; i < len(kv.main)-1; i++ {
		if kv.shouldMerge(i) {
			if err := kv.compactSSTables(i); err != nil {
				return err
			}
			i--
		}
	}
	return nil
}

// Turn log (mirror by MemTable) into the top-level SSTable
func (kv *KV) compactLog() error {
	kv.version++
	sstable := fmt.Sprintf("sstable_%d", kv.version)
	filename := path.Join(kv.Options.Dirpath, sstable)
	file := SortedFile{FileName: filename}

	m := SortedKV(MergedSortedKV{&kv.mem})
	if len(kv.main) == 0 {
		// last level (after compact) must drop deleted keys
		m = NoDeletedSortedKV{m}
	}

	if err := file.CreateFromSorted(m); err != nil {
		_ = os.Remove(filename)
		return err
	}

	// record new SSTable name
	metadata := kv.meta.Get()
	metadata.Version = kv.version
	metadata.SSTables = slices.Insert(metadata.SSTables, 0, sstable)
	if err := kv.meta.Set(metadata); err != nil {
		// don't remove new SSTable since metadata may have been persisted
		// if we remove it, the next open will try to read a non-existing file
		_ = file.Close()
		return err
	}

	kv.main = slices.Insert(kv.main, 0, file)
	kv.mem.Clear()
	return kv.log.Truncate()
}

// decide whether to merge level idx with idx+1
func (kv *KV) shouldMerge(idx int) bool {
	check(idx+1 < len(kv.main))
	currSize, nextSize := kv.main[idx].EstimatedSize(), kv.main[idx+1].EstimatedSize()
	return float32(currSize)*kv.Options.GrowthFactor >= float32(nextSize+currSize)
}

// merge 2 adjacent SSTables (level and level+1) and replace the originals
func (kv *KV) compactSSTables(level int) error {
	kv.version++
	sstable := fmt.Sprintf("sstable_%d", kv.version)
	filename := path.Join(kv.Options.Dirpath, sstable)
	file := SortedFile{FileName: filename}

	m := SortedKV(MergedSortedKV([]SortedKV{&kv.main[level], &kv.main[level+1]}))
	if level == len(kv.main)-2 {
		// last level (after merge) must drop deleted keys
		m = NoDeletedSortedKV{m}
	}

	if err := file.CreateFromSorted(m); err != nil {
		_ = os.Remove(filename)
		return err
	}

	// record new file name
	metadata := kv.meta.Get()
	metadata.Version = kv.version
	metadata.SSTables = slices.Replace(metadata.SSTables, level, level+2, sstable)
	if err := kv.meta.Set(metadata); err != nil {
		_ = file.Close()
		return err
	}

	old1, old2 := kv.main[level].FileName, kv.main[level+1].FileName
	kv.main = slices.Replace(kv.main, level, level+2, file)

	// delete old files
	_ = os.Remove(old1)
	_ = os.Remove(old2)

	return nil
}

type NoDeletedSortedKV struct {
	SortedKV
}

func (kv NoDeletedSortedKV) Iter() (iter SortedKVIter, err error) {
	if iter, err = kv.SortedKV.Iter(); err != nil {
		return nil, err
	}
	return NoDeletedIter{iter}, nil
}
