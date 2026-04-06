package db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"
)

type SortedKV interface {
	EstimatedSize() int
	Iter() (SortedKVIter, error)
	Seek(key []byte) (SortedKVIter, error)
}

type SortedKVIter interface {
	Valid() bool
	Key() []byte
	Val() []byte
	Deleted() bool
	Next() error
	Prev() error
}

// SSTable (sorted string table)
type SortedFile struct {
	FileName string
	fp       *os.File
	nkeys    int
}

func (file *SortedFile) EstimatedSize() int {
	return file.nkeys
}

func (file *SortedFile) Close() error {
	if file.fp == nil {
		return nil
	}
	return file.fp.Close()
}

func (file *SortedFile) Open() (err error) {
	file.fp, err = os.OpenFile(file.FileName, os.O_RDONLY, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err = file.openExisting(); err != nil {
		_ = file.Close()
	}
	return err
}

func (file *SortedFile) openExisting() error {
	var buf [8]byte
	if _, err := file.fp.ReadAt(buf[:8], 0); err != nil {
		return err
	}
	file.nkeys = int(binary.LittleEndian.Uint64(buf[:8]))
	return nil
}

/*
SSTable format:
[ n keys | offset1 | ... | offsetn | gap | KV1 | ... | KVn ]
  8 bytes  8 bytes

- Over-allocating the offset array creates unused gap in the file.
  But this gap doesn't waste disk space due to 'sparse file' feature.

KV format:
[ key length | val length | key data | val data ]
  4 bytes      4 bytes
*/

/* Create SSTable by flushing the in-memory data structure (MemTable). */
func (file *SortedFile) CreateFromSorted(kv SortedKV) (err error) {
	if file.fp, err = createFileSync(file.FileName); err != nil {
		return err
	}
	if err = file.writeSortedFile(kv); err != nil {
		_ = file.Close()
	}
	return err
}

// Wrap WriteAt as an io.Writer interface for sequential writes
type OffsetWriter struct {
	writer io.WriterAt
	offset int64
}

func (w *OffsetWriter) Write(data []byte) (n int, err error) {
	n, err = w.writer.WriteAt(data, w.offset)
	w.offset += int64(n)
	return n, err
}

func (file *SortedFile) writeSortedFile(kv SortedKV) (err error) {
	iter, err := kv.Iter()
	if err != nil {
		return err
	}

	// - os.File Write() map to OS syscall.
	//   (write to OS page cache, OS decides when to flush to disk, unless 'fsync' is used)
	//   Each syscall has overhead.
	//   -> Use 'bufio' to buffer writes at the application level.
	// - One problem is bufio.Writer assumes sequential writes,
	//   while we want to use WriteAt.
	//   -> Wrap the os.File with OffsetWriter before creating bufio.Writer
	//      (os.File implements io.WriterAt).
	// - Write both the offset array and KV data in a single pass,
	//   using 2 bufio.Writer for 2 regions.

	offsetRegionWriter := bufio.NewWriter(&OffsetWriter{
		writer: file.fp,
		offset: 8, // fill nkeys later
	})

	offsetRegionSize := 8 * (1 + kv.EstimatedSize())
	kvRegionWriter := bufio.NewWriter(&OffsetWriter{
		writer: file.fp,
		offset: int64(offsetRegionSize),
	})

	nkeys := 0
	offset := offsetRegionSize // offset of current KV

	for ; iter.Valid(); iter.Next() {
		// Skip deleted entries
		if iter.Deleted() {
			continue
		}

		k, v := iter.Key(), iter.Val()
		// Write current KV
		_, err = kvRegionWriter.Write(
			binary.LittleEndian.AppendUint32(nil, uint32(len(k))),
		)
		if err != nil {
			return err
		}
		_, err = kvRegionWriter.Write(
			binary.LittleEndian.AppendUint32(nil, uint32(len(v))),
		)
		if err != nil {
			return err
		}
		_, err = kvRegionWriter.Write(k)
		if err != nil {
			return err
		}
		_, err = kvRegionWriter.Write(v)
		if err != nil {
			return err
		}

		// Write offset of current KV
		_, err = offsetRegionWriter.Write(
			binary.LittleEndian.AppendUint64(nil, uint64(offset)),
		)
		if err != nil {
			return err
		}

		nkeys++
		offset += 4 + 4 + len(k) + len(v) // offset for next KV
	}

	check(nkeys <= kv.EstimatedSize())
	file.nkeys = nkeys
	// Write number of keys (to OS page cache)
	file.fp.WriteAt(binary.LittleEndian.AppendUint64(nil, uint64(nkeys)), 0)

	// Write buffered data to OS page cache
	err = offsetRegionWriter.Flush()
	if err != nil {
		return err
	}
	err = kvRegionWriter.Flush()
	if err != nil {
		return err
	}

	// fsync (write to disk)
	return file.fp.Sync()
}

func (file *SortedFile) Size() int {
	return file.nkeys
}

func (file *SortedFile) Iter() (SortedKVIter, error) {
	iter := &SortedFileIter{file: file, pos: 0}
	if err := iter.loadCurrent(); err != nil {
		return nil, err
	}
	return iter, nil
}

func (file *SortedFile) Seek(key []byte) (SortedKVIter, error) {
	pos, err := file.binarySearch(key, 0, file.nkeys-1)
	if err != nil {
		return nil, err
	}

	iter := &SortedFileIter{file: file, pos: pos}
	if err = iter.loadCurrent(); err != nil {
		return nil, err
	}
	return iter, nil
}

/* Find index of the first entry >= key. */
func (file *SortedFile) binarySearch(
	key []byte, left int, right int,
) (pos int, err error) {
	for left <= right {
		mid := (left + right) / 2 // floor division
		k, _, err := file.index(mid)
		if err != nil {
			return -1, err
		}

		r := bytes.Compare(k, key)
		if r == 0 {
			return mid, nil
		} else if r < 0 {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return left, nil
}

type SortedFileIter struct {
	file *SortedFile
	pos  int
	key  []byte
	val  []byte
}

func (iter *SortedFileIter) Valid() bool {
	return 0 <= iter.pos && iter.pos < iter.file.nkeys
}

func (iter *SortedFileIter) Key() []byte {
	return iter.key
}

func (iter *SortedFileIter) Val() []byte {
	return iter.val
}

func (iter *SortedFileIter) Deleted() bool {
	return false
}

func (iter *SortedFileIter) Next() error {
	if iter.pos < iter.file.nkeys {
		iter.pos++
	}
	return iter.loadCurrent()
}

func (iter *SortedFileIter) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}
	return iter.loadCurrent()
}

func (iter *SortedFileIter) loadCurrent() (err error) {
	if iter.Valid() {
		iter.key, iter.val, err = iter.file.index(iter.pos)
	}
	return err
}

/* Read the pos-th KV pair. */
func (file *SortedFile) index(pos int) (key []byte, val []byte, err error) {
	// Read KV offset
	var buf [8]byte
	if _, err = file.fp.ReadAt(buf[:], int64(8+8*pos)); err != nil {
		return nil, nil, err
	}
	offset := int64(binary.LittleEndian.Uint64(buf[:]))
	if offset < int64(8+8*file.nkeys) {
		return nil, nil, errors.New("corrupted file")
	}

	// Read KV metadata
	if _, err = file.fp.ReadAt(buf[:], offset); err != nil {
		return nil, nil, err
	}
	k_len := int(binary.LittleEndian.Uint32(buf[:4]))
	v_len := int(binary.LittleEndian.Uint32(buf[4:]))

	// Read KV data
	data := make([]byte, k_len+v_len)
	if _, err = file.fp.ReadAt(data, offset+4+4); err != nil {
		return nil, nil, err
	}
	return data[:k_len], data[k_len:], nil
}
