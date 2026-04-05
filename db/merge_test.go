package db

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/* Convert slice of strings to slice of slice of bytes. */
func slist2blist(list []string) (out [][]byte) {
	for _, v := range list {
		out = append(out, ([]byte)(v))
	}
	return out
}

func testMerge(
	t *testing.T,
	skll ...[]string, // list of list of (string) keys from different levels
) {
	// Prepare expected result
	seenKeys := map[string]bool{}
	expected := []Entry{}
	kll := [][][]byte{} // list of list of ([]byte) keys
	vll := [][][]byte{} // list of list of values

	for i, strKeys := range skll {
		// Convert string keys to []byte keys
		keys := slist2blist(strKeys)
		kll = append(kll, keys)

		// Generate vals
		vals := [][]byte{}
		for range strKeys {
			vals = append(vals, []byte{'A' + byte(i)})
		}
		vll = append(vll, vals)

		// Ignore duplicate keys in lower levels
		for i, key := range strKeys {
			if seenKeys[key] {
				continue
			}
			seenKeys[key] = true
			expected = append(
				expected,
				Entry{key: keys[i], val: vals[i], deleted: false},
			)
		}
	}

	// Sort expected output by key
	slices.SortStableFunc(expected, func(a, b Entry) int {
		return bytes.Compare(a.key, b.key)
	})

	// Setup MergeSortedKV iterator
	seqs := []SortedKV{}
	for i, keys := range kll {
		seq := &SortedArray{keys: keys, vals: vll[i]}
		seqs = append(seqs, seq)
	}
	m := MergedSortedKV(seqs)
	iter, err := m.Iter()

	// Test forward scan
	i := 0
	for ; err == nil && iter.Valid(); err = iter.Next() {
		assert.Equal(t, expected[i].key, iter.Key())
		assert.Equal(t, expected[i].val, iter.Val())
		i++
	}
	require.Nil(t, err)
	assert.False(t, iter.Valid())
	assert.Equal(t, len(expected), i)

	// Test backward scan
	for err = iter.Prev(); err == nil && iter.Valid(); err = iter.Prev() {
		i--
		assert.Equal(t, expected[i].key, iter.Key())
		assert.Equal(t, expected[i].val, iter.Val())
	}
	require.Nil(t, err)
	assert.False(t, iter.Valid())
	assert.Equal(t, 0, i)

	// TODO: Test flipping direction mid way
}

func TestMerge(t *testing.T) {
	a := []string{}
	b := []string{}
	testMerge(t, a, b)

	a = []string{"x", "z"}
	b = []string{}
	testMerge(t, a, b)

	a = []string{}
	b = []string{"x", "z"}
	testMerge(t, a, b)

	a = []string{"x", "z"}
	b = []string{"x", "z"} // duplicate keys
	testMerge(t, a, b)

	a = []string{"x", "z"}
	b = []string{"w", "y"}
	testMerge(t, a, b)

	a, b = b, a
	testMerge(t, a, b)
}
