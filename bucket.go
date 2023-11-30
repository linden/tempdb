package tempdb

import (
	"bytes"
	"errors"

	"github.com/btcsuite/btcwallet/walletdb"
)

type Bucket struct {
	tx *Transaction

	ID     BucketID
	Parent BucketID

	Key   []byte
	Value map[string][]byte

	sequence uint64
}

func (bkt *Bucket) Put(key, value []byte) error {
	Logger.Debug("bucket put", "key", key, "value", value, "bucket ID", bkt.ID)

	if key == nil {
		return walletdb.ErrKeyRequired
	}

	bkt.Value[string(key)] = value
	return nil
}

func (bkt *Bucket) Get(key []byte) []byte {
	Logger.Debug("bucket get", "key", key, "bucket ID", bkt.ID)

	return bkt.Value[string(key)]
}

func (bkt *Bucket) Delete(key []byte) error {
	Logger.Debug("bucket delete", "key", key, "bucket ID", bkt.ID)

	delete(bkt.Value, string(key))
	return nil
}

func (bkt *Bucket) ForEach(fn func(k, v []byte) error) error {
	Logger.Debug("bucket for each", "bucket ID", bkt.ID)

	c := newCursor(bkt)

	for k, v := c.First(); k != nil; k, v = c.Next() {
		err := fn([]byte(k), v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bkt *Bucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	Logger.Debug("create nested read bucket", "key", key, "parent ID", bkt.ID)

	return bkt.NestedReadWriteBucket(key)
}

func (bkt *Bucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	Logger.Debug("create nested read/write bucket", "key", key, "parent ID", bkt.ID)

	_, nbkt, ok := bkt.find(key)
	if !ok {
		return nil
	}

	return nbkt
}

func (bkt *Bucket) CreateBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	Logger.Debug("create bucket", "key", key, "parent ID", bkt.ID)

	if key == nil {
		return nil, walletdb.ErrBucketNameRequired
	}

	_, _, ok := bkt.find(key)
	if ok {
		return nil, walletdb.ErrBucketExists
	}

	// add a new empty value. used for iteration.
	bkt.Put(key, nil)

	return bkt.tx.createBucket(key, bkt.ID), nil
}

func (bkt *Bucket) CreateBucketIfNotExists(key []byte) (walletdb.ReadWriteBucket, error) {
	Logger.Debug("create bucket (if not exists)", "key", key, "parent ID", bkt.ID)

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
	Logger.Debug("delete nested bucket", "key", key, "parent ID", bkt.ID)

	if key == nil {
		return walletdb.ErrIncompatibleValue
	}

	i, _, ok := bkt.find(key)
	if !ok {
		return walletdb.ErrBucketNotFound
	}

	bkt.tx.State.Buckets = append(bkt.tx.State.Buckets[:i], bkt.tx.State.Buckets[i+1:]...)

	// delete the empty value.
	bkt.Delete(key)

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
	Logger.Debug("create read cursor", "parent ID", bkt.ID)

	return newCursor(bkt)
}

func (bkt *Bucket) ReadWriteCursor() walletdb.ReadWriteCursor {
	Logger.Debug("create read/write cursor", "parent ID", bkt.ID)

	return newCursor(bkt)
}

func (bkt *Bucket) find(key []byte) (int, *Bucket, bool) {
	for i, nbkt := range bkt.tx.State.Buckets {
		// ensure the bucket is not the root bucket.
		if nbkt.Parent == RootBucketID {
			continue
		}

		// ensure the bucket is nested in the current bucket.
		if nbkt.Parent != bkt.ID {
			continue
		}

		// ensure the key matches.
		if !bytes.Equal(nbkt.Key, key) {
			continue
		}

		return i, &nbkt, true
	}

	return 0, nil, false
}
