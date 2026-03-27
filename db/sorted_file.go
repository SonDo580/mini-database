package db

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type SortedKV interface {
	Size() int
	Iter() (SortedKVIter, error)
}

type SortedKVIter interface {
	Valid() bool
	Key() []byte
	Val() []byte
	Next() error
	Prev() error
}

// SSTable (sorted string table)
type SortedFile struct {
	FileName string
	fp       *os.File
}

func (file *SortedFile) Close() error {
	return file.fp.Close()
}

/*
SSTable format:
[ n keys | offset1 | ... | offsetn | KV1 | ... | KVn ]
  8 bytes  8 bytes

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
	kvSize := kv.Size()
	sortedKVIter, err := kv.Iter()
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
		offset: 0,
	})
	offsetRegionSize := 8 * (1 + kvSize)

	kvRegionWriter := bufio.NewWriter(&OffsetWriter{
		writer: file.fp,
		offset: int64(offsetRegionSize),
	})

	var nn int
	offsetWritten := 0 // count number of bytes written to offset region

	// Write number of keys
	nn, err = offsetRegionWriter.Write(
		binary.LittleEndian.AppendUint64(nil, uint64(kvSize)),
	)
	if err != nil {
		return err
	}
	offsetWritten += nn

	// Write offset of the first KV
	nn, err = offsetRegionWriter.Write(
		binary.LittleEndian.AppendUint64(nil, uint64(offsetRegionSize)),
	)
	if err != nil {
		return err
	}
	offsetWritten += nn

	for ; sortedKVIter.Valid(); sortedKVIter.Next() {
		k := sortedKVIter.Key()
		v := sortedKVIter.Val()

		// Write offset of the next KV
		if offsetWritten < offsetRegionSize {
			nn, err = offsetRegionWriter.Write(
				binary.LittleEndian.AppendUint64(nil,
					uint64(offsetRegionSize+4+4+len(k)+len(v))),
			)
			if err != nil {
				return err
			}
			offsetWritten += nn
		}

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
	}

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
