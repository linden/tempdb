package tempdb

import (
	"bytes"
	"testing"

	"github.com/btcsuite/btcwallet/walletdb"
)

func TestBucketForEach(t *testing.T) {
	db, err := New("bucket-for-each.db")
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx walletdb.ReadWriteTx) error {
		// create the root bucket.
		bkt, err := tx.CreateTopLevelBucket([]byte("alphabet"))
		if err != nil {
			t.Fatal(err)
		}

		vals := [][]byte{
			[]byte("a"),
			[]byte("b"),
			[]byte("c"),
		}

		// create all the nested buckets.
		for _, v := range vals {
			_, err = bkt.CreateBucket(v)
			if err != nil {
				t.Fatal(err)
			}
		}

		var i int

		// iterate over every bucket.
		return bkt.ForEach(func(k, v []byte) error {
			if !bytes.Equal(k, vals[i]) {
				t.Fatalf("expected %s but got %s", k, vals[i])
			}

			if v != nil {
				t.Fatalf("expected a value of nil but got %v", v)
			}

			i++

			return nil
		})
	}, reset)
	if err != nil {
		t.Fatal(err)
	}
}
