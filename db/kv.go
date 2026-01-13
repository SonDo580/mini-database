package db

import "bytes"

type KV struct {
	log Log
	mem map[string][]byte
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}

	kv.mem = map[string][]byte{}
	for {
		ent := Entry{}
		eof, err := kv.log.Read(&ent)
		if err != nil {
			return err
		}
		if eof {
			break
		}

		if ent.deleted {
			delete(kv.mem, string(ent.key))
		} else {
			kv.mem[string(ent.key)] = ent.val
		}
	}

	return nil
}

func (kv *KV) Close() error {
	return kv.log.Close()
}

func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	val, ok = kv.mem[string(key)]
	return
}

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0 // insert or update
	ModeInsert UpdateMode = 1 // insert new
	ModeUpdate UpdateMode = 2 // update existing
)

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (updated bool, err error) {
	curr_val, exists := kv.mem[string(key)]

	var needUpdate bool
	if mode == ModeInsert {
		needUpdate = !exists
	} else if mode == ModeUpdate {
		needUpdate = exists && !bytes.Equal(curr_val, val)
	} else {
		needUpdate = !exists || !bytes.Equal(curr_val, val)
	}

	if needUpdate {
		err = kv.log.Write(&Entry{key: key, val: val})
		if err != nil {
			return
		}

		kv.mem[string(key)] = val
		updated = true
	}

	return
}

func (kv *KV) Set(key []byte, val []byte) (updated bool, err error) {
	return kv.SetEx(key, val, ModeUpsert)
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	_, exists := kv.mem[string(key)]
	if exists {
		err = kv.log.Write(&Entry{key: key, deleted: true})
		if err != nil {
			deleted = false
			return
		}

		delete(kv.mem, string(key))
		deleted = true
	}
	return
}
