package db

import "bytes"

// Combine different levels (MemTable, SSTables)
type MergedSortedKV []SortedKV

func (m MergedSortedKV) Iter() (iter SortedKVIter, err error) {
	levels := make([]SortedKVIter, len(m))
	for i, sub := range m {
		if levels[i], err = sub.Iter(); err != nil {
			return nil, err
		}
	}
	return &MergedSortedKVIter{
		levels: levels,
		which:  levelsSmallest(levels),
	}, nil
}

type MergedSortedKVIter struct {
	levels []SortedKVIter

	// 'which' records iterator with current smallest or largest key.
	// Calculation requirements:
	// - prioritize upper levels.
	// - do not output duplicate keys.
	// - direction can change midway.
	which int
}

func (iter *MergedSortedKVIter) Valid() bool {
	return iter.which >= 0
}

func (iter *MergedSortedKVIter) Key() []byte {
	return iter.levels[iter.which].Key()
}

func (iter *MergedSortedKVIter) Val() []byte {
	return iter.levels[iter.which].Val()
}

func (iter *MergedSortedKVIter) Next() error {
	currKey := []byte(nil) // indicate no current key
	if iter.Valid() {
		currKey = iter.Key()
	}

	// For each sub-iterator:
	// - If it's invalid, call Next():
	//   . case 1: move from 'before start' to the first valid key.
	//   . case 2: no-op at 'after end'.
	// - If its current key == currKey, also call Next():
	//   . case 1: go to the next key in current 'which' level.
	//   . case 2: skip the same key in levels lower than 'which' (avoid returning duplicates).
	for _, sub := range iter.levels {
		if !sub.Valid() || bytes.Compare(sub.Key(), currKey) == 0 {
			if err := sub.Next(); err != nil {
				return err
			}
		}
	}
	iter.which = levelsSmallest(iter.levels)
	return nil
}

func (iter *MergedSortedKVIter) Prev() error {
	// Similar logic to Next(), but in reverse direction

	currKey := []byte(nil)
	if iter.Valid() {
		currKey = iter.Key()
	}

	for _, sub := range iter.levels {
		if !sub.Valid() || bytes.Compare(sub.Key(), currKey) == 0 {
			if err := sub.Prev(); err != nil {
				return err
			}
		}
	}
	iter.which = levelsHighest(iter.levels)
	return nil
}

/* Find level with current smallest key. */
func levelsSmallest(levels []SortedKVIter) int {
	winIdx := -1
	winKey := []byte(nil)
	for i, sub := range levels {
		if !sub.Valid() {
			continue
		}
		key := sub.Key()
		// Only update "winner" if key < winKey.
		// Don't update if key == winKey since we prioritize upper levels.
		if winIdx == -1 || bytes.Compare(key, winKey) < 0 {
			winIdx, winKey = i, key
		}
	}
	return winIdx
}

/* Find level with current highest key. */
func levelsHighest(levels []SortedKVIter) int {
	winIdx := -1
	winKey := []byte(nil)
	for i, sub := range levels {
		if !sub.Valid() {
			continue
		}
		key := sub.Key()
		// Only update "winner" if key > winKey.
		// Don't update if key == winKey since we prioritize upper levels.
		if winIdx == -1 || bytes.Compare(key, winKey) > 0 {
			winIdx, winKey = i, key
		}
	}
	return winIdx
}
