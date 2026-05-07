package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKVBasic(t *testing.T) {
	kv := KV{}
	kv.Options.Dirpath = "test_db"
	defer os.RemoveAll(kv.Options.Dirpath)

	os.RemoveAll(kv.Options.Dirpath)
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	updated, err := kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, updated && err == nil)

	val, ok, err := kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v1" && ok && err == nil)

	_, ok, err = kv.Get([]byte("xxx"))
	assert.True(t, !ok && err == nil)

	deleted, err := kv.Del([]byte("xxx"))
	assert.True(t, !deleted && err == nil)

	deleted, err = kv.Del([]byte("k1"))
	assert.True(t, deleted && err == nil)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)

	updated, err = kv.Set([]byte("k2"), []byte("v2"))
	assert.True(t, updated && err == nil)

	updated, err = kv.Set([]byte("k3"), []byte("v3"))
	assert.True(t, updated && err == nil)

	// Reopen
	kv.Close()
	err = kv.Open()
	require.Nil(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)

	val, ok, err = kv.Get([]byte("k2"))
	assert.True(t, string(val) == "v2" && ok && err == nil)

	// Compact
	err = kv.Compact()
	require.Nil(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)

	val, ok, err = kv.Get([]byte("k2"))
	assert.True(t, string(val) == "v2" && ok && err == nil)

	updated, err = kv.Set([]byte("k2"), []byte("v2"))
	assert.True(t, !updated && err == nil)

	deleted, err = kv.Del([]byte("k3"))
	assert.True(t, deleted && err == nil)

	_, ok, err = kv.Get([]byte("k3"))
	assert.True(t, !ok && err == nil)

	// Reopen
	kv.Close()
	err = kv.Open()
	require.Nil(t, err)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)

	val, ok, err = kv.Get([]byte("k2"))
	assert.True(t, string(val) == "v2" && ok && err == nil)

	_, ok, err = kv.Get([]byte("k3"))
	assert.True(t, !ok && err == nil)
}

func TestKVReopen(t *testing.T) {
	path := "test_db"
	defer os.RemoveAll(path)

	for mode := 0; mode < 3; mode++ {
		// mode = 0: compact
		// mode = 1: compact & reopen
		// mode = 2: reopen

		os.RemoveAll(path)
		kv := KV{Options: KVOptions{Dirpath: path}}
		err := kv.Open()
		require.Nil(t, err)

		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("data%d", i))
			val := key
			updated, err := kv.Set(key, val)
			require.Nil(t, err)
			require.True(t, updated)

			if mode == 0 || mode == 1 {
				err = kv.Compact()
				require.Nil(t, err)
			}
			if mode == 1 || mode == 2 {
				err = kv.Close()
				require.Nil(t, err)
				err = kv.Open()
				require.Nil(t, err)
			}

			for j := 0; j < i; j++ {
				key := []byte(fmt.Sprintf("data%d", j))
				expected_val := key
				val, ok, err := kv.Get(key)
				assert.True(t, err == nil && ok && string(val) == string(expected_val))
			}
		}

		err = kv.Close()
		require.Nil(t, err)
	}

}

func TestKVUpdateMode(t *testing.T) {
	kv := KV{}
	kv.Options.Dirpath = "test_db"
	defer os.RemoveAll(kv.Options.Dirpath)

	os.RemoveAll(kv.Options.Dirpath)
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	updated, err := kv.SetEx([]byte("k1"), []byte("v1"), ModeUpdate)
	assert.True(t, !updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("v1"), ModeInsert)
	assert.True(t, updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("v11"), ModeInsert)
	assert.True(t, !updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("v12"), ModeUpdate)
	assert.True(t, updated && err == nil)

	updated, err = kv.SetEx([]byte("k1"), []byte("v13"), ModeUpsert)
	assert.True(t, updated && err == nil)

	updated, err = kv.SetEx([]byte("k2"), []byte("v2"), ModeUpsert)
	assert.True(t, updated && err == nil)
}

func TestKVRecovery(t *testing.T) {
	kv := KV{}
	kv.Options.Dirpath = "test_db"
	defer os.RemoveAll(kv.Options.Dirpath)

	prepare := func() {
		os.RemoveAll(kv.Options.Dirpath)

		err := kv.Open()
		assert.Nil(t, err)
		defer kv.Close()

		updated, err := kv.Set([]byte("k1"), []byte("v1"))
		assert.True(t, updated && err == nil)
		updated, err = kv.Set([]byte("k2"), []byte("v2"))
		assert.True(t, updated && err == nil)
	}

	// simulate truncated log
	prepare()
	fp, _ := os.OpenFile(kv.log.FileName, os.O_RDWR, 0o644)
	stat, _ := fp.Stat()
	fp.Truncate(stat.Size() - 1)
	fp.Close()
	// reopen
	err := kv.Open()
	assert.Nil(t, err)
	// test
	val, ok, err := kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v1" && ok && err == nil)
	_, ok, err = kv.Get([]byte("k2")) // bad
	assert.True(t, !ok && err == nil)
	kv.Close()

	// simulate corrupted data
	prepare()
	fp, _ = os.OpenFile(kv.log.FileName, os.O_RDWR, 0o644)
	stat, _ = fp.Stat()
	fp.WriteAt([]byte{0}, stat.Size()-1)
	fp.Close()
	// reopen
	err = kv.Open()
	assert.Nil(t, err)
	// test
	val, ok, err = kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v1" && ok && err == nil)
	_, ok, err = kv.Get([]byte("k2")) // bad
	assert.True(t, !ok && err == nil)
	kv.Close()
}

func TestEntryEncodeDecode(t *testing.T) {
	ent := Entry{key: []byte("k1"), val: []byte("zero")}
	encoded := []byte{0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0, 0, 'k', '1', 'z', 'e', 'r', 'o'}
	binary.LittleEndian.PutUint32(encoded[:4], crc32.ChecksumIEEE(encoded[4:]))
	assert.Equal(t, ent.Encode(), encoded)

	decoded := Entry{}
	err := decoded.Decode(bytes.NewBuffer(encoded))
	assert.Nil(t, err)
	assert.Equal(t, ent, decoded)

	ent = Entry{key: []byte("k1"), val: []byte("zero"), deleted: true}
	encoded = []byte{0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 'k', '1'}
	binary.LittleEndian.PutUint32(encoded[:4], crc32.ChecksumIEEE(encoded[4:]))
	assert.Equal(t, ent.Encode(), encoded)

	ent = Entry{key: []byte("k1"), deleted: true}
	assert.Equal(t, ent.Encode(), encoded)

	decoded = Entry{}
	err = decoded.Decode(bytes.NewBuffer(encoded))
	assert.Nil(t, err)
	assert.Equal(t, ent, decoded)
}

func TestKVSeek(t *testing.T) {
	kv := KV{}
	kv.Options.Dirpath = "test_db"
	defer os.RemoveAll(kv.Options.Dirpath)

	os.RemoveAll(kv.Options.Dirpath)
	err := kv.Open()
	assert.Nil(t, err)
	defer kv.Close()

	keys := []string{"c", "e", "g"}
	vals := []string{"3", "5", "7"}
	for i := range keys {
		_, _ = kv.Set([]byte(keys[i]), []byte(vals[i]))
	}

	iter, err := kv.Seek([]byte("a"))
	require.Nil(t, err)
	for i := range keys {
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte(keys[i]), iter.Key())
		assert.Equal(t, []byte(vals[i]), iter.Val())
		err = iter.Next()
		require.Nil(t, err)
	}
	assert.False(t, iter.Valid())

	err = iter.Prev()
	require.Nil(t, err)
	for i := len(keys) - 1; i >= 0; i-- {
		assert.True(t, iter.Valid())
		assert.Equal(t, []byte(keys[i]), iter.Key())
		assert.Equal(t, []byte(vals[i]), iter.Val())
		err = iter.Prev()
		require.Nil(t, err)
	}
	assert.False(t, iter.Valid())

	iter, err = kv.Seek([]byte("f"))
	require.Nil(t, err)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("g"), iter.Key())

	iter, err = kv.Seek([]byte("g"))
	require.Nil(t, err)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("g"), iter.Key())

	iter, err = kv.Seek([]byte("h"))
	require.Nil(t, err)
	assert.False(t, iter.Valid())
}
