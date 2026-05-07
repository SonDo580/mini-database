package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedFile(t *testing.T) {
	sf := SortedFile{FileName: ".test_sorted_file"}
	defer os.Remove(sf.FileName)

	keys := [][]byte{[]byte("x"), []byte("x2"), []byte("y")}
	vals := [][]byte{[]byte("1"), []byte(""), []byte("234")}
	deleted := []bool{false, true, false}
	err := sf.CreateFromSorted(&SortedArray{keys, vals, deleted})
	require.Nil(t, err)
	defer sf.Close()
	assert.Equal(t, 3, sf.EstimatedSize())

	expected := []byte{
		3, 0, 0, 0, 0, 0, 0, 0, // n_keys = 3
		32, 0, 0, 0, 0, 0, 0, 0, // offset_0 = (n_keys + 1)*8
		43, 0, 0, 0, 0, 0, 0, 0, // offset_i+1 = offset_i + 4+4+1 + len(key_i) + len(val_i)
		54, 0, 0, 0, 0, 0, 0, 0,
		1, 0, 0, 0, 1, 0, 0, 0, 0, 'x', '1',
		2, 0, 0, 0, 0, 0, 0, 0, 1, 'x', '2',
		1, 0, 0, 0, 3, 0, 0, 0, 0, 'y', '2', '3', '4',
	}
	data, err := os.ReadFile(sf.FileName)
	require.Nil(t, err)
	assert.Equal(t, expected, data)

	iter, err := sf.Iter()
	i := 0
	for ; err == nil && iter.Valid(); err = iter.Next() {
		assert.Equal(t, keys[i], iter.Key())
		assert.Equal(t, vals[i], iter.Val())
		i++
	}
	require.Nil(t, err)

	iter, err = sf.Seek([]byte("xx"))
	require.Nil(t, err)
	assert.True(t, iter.Valid())
	assert.Equal(t, []byte("y"), iter.Key())
}
