package tempdb

import (
	"bytes"
	"errors"

	"github.com/btcsuite/btcwallet/walletdb"
)

type Bucket struct {
	tx *Transaction

	id     BucketID
	parent BucketID

	key   []byte
	value map[string][]byte

	sequence uint64
}

func (bkt *Bucket) Put(key, value []byte) error {
	if key == nil {
		return walletdb.ErrKeyRequired
	}

	bkt.value[string(key)] = value
	return nil
}

func (bkt *Bucket) Get(key []byte) []byte {
	return bkt.value[string(key)]
}

func (bkt *Bucket) Delete(key []byte) error {
	delete(bkt.value, string(key))
	return nil
}

func (bkt *Bucket) ForEach(f func(k, v []byte) error) error {
	for k, v := range bkt.value {
		err := f([]byte(k), v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bkt *Bucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	return bkt.NestedReadWriteBucket(key)
}

func (bkt *Bucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	_, nbkt, ok := bkt.find(key)
	if !ok {
		return nil
	}

	return nbkt
}

func (bkt *Bucket) CreateBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	if key == nil {
		return nil, walletdb.ErrBucketNameRequired
	}

	_, _, ok := bkt.find(key)
	if ok {
		return nil, walletdb.ErrBucketExists
	}

	return bkt.tx.createBucket(key, bkt.id), nil
}

func (bkt *Bucket) CreateBucketIfNotExists(key []byte) (walletdb.ReadWriteBucket, error) {
	nbkt := bkt.NestedReadWriteBucket(key)
	if nbkt != nil {
		return nbkt, nil
	}

	nbkt, err := bkt.CreateBucket(key)
	if err != nil && !errors.Is(err, walletdb.ErrBucketExists) {
		return nil, err
	}

	return nbkt, nil
}

func (bkt *Bucket) DeleteNestedBucket(key []byte) error {
	if key == nil {
		return walletdb.ErrIncompatibleValue
	}

	i, _, ok := bkt.find(key)
	if !ok {
		return walletdb.ErrBucketNotFound
	}

	bkt.tx.state.buckets = append(bkt.tx.state.buckets[:i], bkt.tx.state.buckets[i+1:]...)

	return nil
}

func (bkt *Bucket) Tx() walletdb.ReadWriteTx {
	return bkt.tx
}

func (bkt *Bucket) NextSequence() (uint64, error) {
	bkt.sequence++
	return bkt.sequence, nil
}

func (bkt *Bucket) SetSequence(v uint64) error {
	bkt.sequence = v
	return nil
}

func (bkt *Bucket) Sequence() uint64 {
	return bkt.sequence
}

func (bkt *Bucket) ReadCursor() walletdb.ReadCursor {
	return newCursor(bkt)
}

func (bkt *Bucket) ReadWriteCursor() walletdb.ReadWriteCursor {
	return newCursor(bkt)
}

func (bkt *Bucket) find(key []byte) (int, *Bucket, bool) {
	for i, nbkt := range bkt.tx.state.buckets {
		// ensure the bucket is not the root bucket.
		if nbkt.parent == RootBucketID {
			continue
		}

		// ensure the bucket is nested in the current bucket.
		if nbkt.parent != bkt.id {
			continue
		}

		// ensure the key matches.
		if !bytes.Equal(nbkt.key, key) {
			continue
		}

		return i, &nbkt, true
	}

	return 0, nil, false
}