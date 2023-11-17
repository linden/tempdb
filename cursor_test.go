package tempdb

import (
	"testing"

	"github.com/btcsuite/btcwallet/walletdb"
)

var reset = func() {}

// `walletdbtest.TestInterface` doesn't cover cursors, so we test them here instead.
func TestCursor(t *testing.T) {
	db, err := New("test.db")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx walletdb.ReadWriteTx) error {
		bkt, err := tx.CreateTopLevelBucket([]byte("trees"))
		if err != nil {
			return err
		}

		err = bkt.Put([]byte("c"), nil)
		if err != nil {
			return err
		}

		err = bkt.Put([]byte("b"), nil)
		if err != nil {
			return err
		}

		err = bkt.Put([]byte("a"), nil)
		if err != nil {
			return err
		}

		err = bkt.Put([]byte("3"), nil)
		if err != nil {
			return err
		}

		err = bkt.Put([]byte("2"), nil)
		if err != nil {
			return err
		}

		err = bkt.Put([]byte("1"), nil)
		if err != nil {
			return err
		}

		ex := []string{
			"1",
			"2",
			"3",
			"a",
			"b",
			"c",
		}

		c := bkt.ReadCursor()

		i := 0

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if ex[i] != string(k) {
				t.Fatalf("unexpected order: expected %s got %s", ex[i], k)
			}

			i++
		}

		return nil
	}, reset)
	if err != nil {
		t.Fatal(err)
	}
}
