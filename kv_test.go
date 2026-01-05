package db

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKVBasic(t *testing.T) {
	kv := KV{}
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

	updated, err = kv.Set([]byte("k1"), []byte("v1"))
	assert.True(t, !updated && err == nil)

	updated, err = kv.Set([]byte("k1"), []byte("v12"))
	assert.True(t, updated && err == nil)

	val, ok, err = kv.Get([]byte("k1"))
	assert.True(t, string(val) == "v12" && ok && err == nil)

	deleted, err = kv.Del([]byte("k1"))
	assert.True(t, deleted && err == nil)

	_, ok, err = kv.Get([]byte("k1"))
	assert.True(t, !ok && err == nil)
}

func TestEntryEncodeDecode(t *testing.T) {
	ent := Entry{key: []byte("k1"), val: []byte("zero")}
	encoded := []byte{2, 0, 0, 0, 4, 0, 0, 0, 'k', '1', 'z', 'e', 'r', 'o'}
	assert.Equal(t, ent.Encode(), encoded)

	decoded := Entry{}
	err := decoded.Decode(bytes.NewBuffer(encoded))
	assert.Nil(t, err)
	assert.Equal(t, ent, decoded)
}
