package db

import "bytes"

type KV struct {
	mem map[string][]byte
}

func (kv *KV) Open() error {
	kv.mem = map[string][]byte{}
	return nil
}

func (kv *KV) Close() error {
	return nil
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	val, ok = kv.mem[string(key)]
	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	current_val, exists := kv.mem[string(key)]
	shouldUpdate := !exists || !bytes.Equal(current_val, val)
	if shouldUpdate {
		kv.mem[string(key)] = val
		updated = true
	}
	return
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	_, exists := kv.mem[string(key)]
	if exists {
		delete(kv.mem, string(key))
		deleted = true
	}
	return
}
