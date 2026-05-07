package db

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
	"slices"
)

/*
Update metadata for all SSTable levels:
- Use double-buffering to achieve atomic updates
- Details:
  . use 2 slots (2 files on disk) with version and checksum;
    data is a list of (SSTable) filenames
  . read: pick the one with higher version (and pass checksum)
  . write: when finish a compaction, write new metadata to older slot;
    if crash, the other slot still remains

Metadata slot file layout:
. | crc32   | len     | data 	  |
    4 bytes   4 bytes   len bytes
						(JSON)
*/

type KVMetaStore struct {
	slots [2]KVMetaItem
	MultiClosers
}

type KVMetaItem struct {
	FileName string
	fp       *os.File
	data     KVMetaData // in-memory mirror
}

type KVMetaData struct {
	Version uint64
	SSTable string // only 1 SSTable for now
}

func (meta *KVMetaStore) Open() error {
	for i := range meta.slots {
		fp, data, err := openMetaFile(meta.slots[i].FileName)
		if err != nil {
			_ = meta.Close()
			return err
		}
		meta.slots[i].fp, meta.slots[i].data = fp, data
		meta.MultiClosers = append(meta.MultiClosers, fp)
	}
	return nil
}

func openMetaFile(filename string) (fp *os.File, data KVMetaData, err error) {
	if fp, err = createFileSync(filename); err != nil {
		return nil, KVMetaData{}, err
	}
	if data, err = readMetaFile(fp); err != nil {
		_ = fp.Close()
		return nil, KVMetaData{}, err
	}
	return fp, data, nil
}

// ignore bad copy (including deserialization error, checksum mismatch)
func readMetaFile(fp *os.File) (data KVMetaData, err error) {
	// read all bytes
	buf, err := io.ReadAll(fp)
	if err != nil {
		return KVMetaData{}, err
	}

	// extract header (data length) and checksum
	if len(buf) <= 8 {
		return KVMetaData{}, nil
	}
	checksum := binary.LittleEndian.Uint32(buf[:4])
	data_len := int(binary.LittleEndian.Uint32(buf[4:]))
	if len(buf) != 8+data_len {
		return KVMetaData{}, nil
	}

	// verify integrity with checksum
	if crc32.ChecksumIEEE(buf[4:]) != checksum {
		return KVMetaData{}, nil
	}

	// deserialize JSON data
	metadata := KVMetaData{}
	if err = json.Unmarshal(buf[8:], &metadata); err != nil {
		return KVMetaData{}, nil
	}

	return metadata, nil
}

func writeMetaFile(fp *os.File, data KVMetaData) error {
	// data
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// header & checksum
	data_len := len(encoded)
	encoded = slices.Concat(make([]byte, 8), encoded)
	binary.LittleEndian.PutUint32(encoded[4:8], uint32(data_len))
	binary.LittleEndian.PutUint32(encoded[0:4], crc32.ChecksumIEEE(encoded[4:]))

	if _, err = fp.WriteAt(encoded, 0); err != nil {
		return err
	}
	return fp.Sync()
}

// return the copy with larger version
func (meta *KVMetaStore) Get() KVMetaData {
	return meta.slots[meta.current()].data
}

func (meta *KVMetaStore) current() int {
	if meta.slots[0].data.Version > meta.slots[1].data.Version {
		return 0
	} else {
		return 1
	}
}

// overwrite the copy with smaller version (and fsync)
func (meta *KVMetaStore) Set(data KVMetaData) error {
	curr := meta.current()
	if err := writeMetaFile(meta.slots[1-curr].fp, data); err != nil {
		return err
	}
	meta.slots[1-curr].data = data // update in-memory mirror
	return nil
}
