package tempdb

import (
	"bytes"
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

func TestCursorSeek(t *testing.T) {
	db, err := New("cursor-seek.db")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx walletdb.ReadWriteTx) error {
		bkt, err := tx.CreateTopLevelBucket([]byte("alphabet"))
		if err != nil {
			t.Fatal(err)
		}

		vals := [][]byte{
			[]byte("a"),
			[]byte("b"),
			[]byte("c"),
		}

		// populate the example values.
		for _, v := range vals {
			bkt.Put(v, nil)
		}

		// create a new read/write cursor.
		c := bkt.ReadWriteCursor()

		k, _ := c.Seek([]byte("b"))

		// ensure we've seeked to the right key.
		if !bytes.Equal(k, []byte("b")) {
			t.Fatalf("expected an key of b not %s", k)
		}

		i := c.(*Cursor).index

		// ensure we've seeked to the 2nd key pair.
		if i != 1 {
			t.Fatalf("expected an index of 1 not %d", i)
		}

		return nil
	}, reset)
	if err != nil {
		t.Fatal(err)
	}
}
