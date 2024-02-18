package tempdb

import (
	"bytes"

	"github.com/btcsuite/btcwallet/walletdb"
)

type Transaction struct {
	State  *State
	cursor *State

	ID int

	listeners []func()

	Rolledback bool
}

func (tx *Transaction) ReadBucket(key []byte) walletdb.ReadBucket {
	return tx.ReadWriteBucket(key)
}

func (tx *Transaction) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	Logger.Debug("read/write top-level bucket", "bucket key", key, "transaction ID", tx.ID)

	for _, bkt := range tx.State.Buckets {
		if bkt.Parent != RootBucketID {
			continue
		}

		if bytes.Equal(bkt.Key, key) {
			return &bkt
		}
	}

	return nil
}

func (tx *Transaction) ForEachBucket(f func(key []byte) error) error {
	Logger.Debug("for each top-level bucket", "transaction ID", tx.ID)

	for _, bkt := range tx.State.Buckets {
		if bkt.Parent != RootBucketID {
			continue
		}

		err := f(bkt.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tx *Transaction) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	Logger.Debug("creating top-level bucket", "bucket key", key, "transaction ID", tx.ID)

	bkt := tx.ReadWriteBucket(key)
	if bkt != nil {
		return bkt, nil
	}

	return tx.createBucket(key, RootBucketID), nil
}

func (tx *Transaction) DeleteTopLevelBucket(key []byte) error {
	Logger.Debug("delete top-level bucket", "bucket key", key, "transaction ID", tx.ID)

	for i, bkt := range tx.State.Buckets {
		if bkt.Parent != RootBucketID {
			continue
		}

		if bytes.Equal(bkt.Key, key) {
			tx.State.Buckets = append(tx.State.Buckets[:i], tx.State.Buckets[i+1:]...)
			break
		}
	}

	return nil
}

func (tx *Transaction) Commit() error {
	Logger.Debug("transaction commit", "transaction", tx, "transaction ID", tx.ID)

	if tx.Rolledback {
		return walletdb.ErrTxClosed
	}

	// copy the transaction state then update the database state.
	*tx.cursor = *tx.State.Copy()

	for _, f := range tx.listeners {
		f()
	}

	return nil
}

func (tx *Transaction) OnCommit(f func()) {
	tx.listeners = append(tx.listeners, f)
}

func (tx *Transaction) Rollback() error {
	Logger.Debug("transaction rollback", "transaction", tx, "transaction ID", tx.ID)

	if tx.Rolledback {
		return walletdb.ErrTxClosed
	}

	tx.Rolledback = true
	return nil
}

func (tx *Transaction) createBucket(key []byte, parent BucketID) *Bucket {
	bkt := Bucket{
		tx: tx,

		Key:   key,
		Value: make(map[string][]byte),

		Parent: parent,
	}

	// create the bucket and use the allocated ID.
	bkt.ID = tx.State.Add(bkt)

	Logger.Debug("create bucket", "bucket key", key, "bucket ID", bkt.ID, "parent ID", parent, "transaction ID", tx.ID)

	return &bkt
}

func newTransaction(state *State) *Transaction {
	// create a new transaction.
	tx := &Transaction{
		cursor: state,
		ID:     state.nextTX,
	}

	// increment to next transaction ID.
	state.nextTX += 1

	Logger.Debug("create transaction", "transaction ID", tx.ID)

	// deep copy the state.
	tx.State = state.Copy()

	// update the underlying transaction in each bucket.
	for i, bkt := range tx.State.Buckets {
		bkt.tx = tx
		tx.State.Buckets[i] = bkt
	}

	return tx
}
