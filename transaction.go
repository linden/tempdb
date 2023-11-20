package tempdb

import (
	"bytes"
	"math/rand"

	"github.com/btcsuite/btcwallet/walletdb"
)

type Transaction struct {
	state  *State
	cursor *State

	// An identification to distinguish transactions for logging and debugging
	id int

	listeners []func()

	rollback bool
}

func (tx *Transaction) ReadBucket(key []byte) walletdb.ReadBucket {
	return tx.ReadWriteBucket(key)
}

func (tx *Transaction) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	Logger.Debug("getting top level bucket", "key", key, "tx id", tx.id)

	for _, bkt := range tx.state.buckets {
		if bkt.parent != RootBucketID {
			continue
		}

		if bytes.Equal(bkt.key, key) {
			return &bkt
		}
	}

	return nil
}

func (tx *Transaction) ForEachBucket(f func(key []byte) error) error {
	for _, bkt := range tx.state.buckets {
		if bkt.parent != RootBucketID {
			continue
		}

		err := f(bkt.key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Transaction) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	Logger.Debug("creating top level bucket", "key", key, "tx id", tx.id)

	return tx.createBucket(key, RootBucketID), nil
}

func (tx *Transaction) DeleteTopLevelBucket(key []byte) error {
	for i, bkt := range tx.state.buckets {
		if bkt.parent != RootBucketID {
			continue
		}

		if bytes.Equal(bkt.key, key) {
			tx.state.buckets = append(tx.state.buckets[:i], tx.state.buckets[i+1:]...)
			break
		}
	}

	return nil
}

func (tx *Transaction) Commit() error {
	Logger.Debug("commiting transaction", "tx", tx, "tx id", tx.id)

	if tx.rollback {
		return walletdb.ErrTxClosed
	}

	// copy the transaction state then update the database state.
	*tx.cursor = *tx.state.Copy()

	for _, f := range tx.listeners {
		f()
	}

	return nil
}

func (tx *Transaction) OnCommit(f func()) {
	tx.listeners = append(tx.listeners, f)
}

func (tx *Transaction) Rollback() error {
	if tx.rollback {
		return walletdb.ErrTxClosed
	}

	tx.rollback = true
	return nil
}

func (tx *Transaction) createBucket(key []byte, parent BucketID) *Bucket {
	bkt := Bucket{
		tx: tx,

		key:   key,
		value: make(map[string][]byte),

		parent: parent,
	}

	// create the bucket and use the allocated ID.
	bkt.id = tx.state.Add(bkt)

	return &bkt
}

func newTransaction(state *State) *Transaction {
	// create a new transaction.
	tx := &Transaction{
		cursor: state,
		id: rand.Int(),
	}

	// deep copy the state.
	tx.state = state.Copy()

	// update the underlying transaction in each bucket.
	for i, bkt := range tx.state.buckets {
		bkt.tx = tx
		tx.state.buckets[i] = bkt
	}

	return tx
}
