package tempdb

import (
	"log/slog"
	"os"
	"strconv"
	"testing"

	"github.com/btcsuite/btcwallet/walletdb/walletdbtest"
)

func TestMain(m *testing.M) {
	// create a new logger.
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	Logger = slog.New(h)

	// run the tests and forward the exit code.
	os.Exit(m.Run())
}

func TestInterface(t *testing.T) {
	Logger = slog.Default()

	walletdbtest.TestInterface(t, "tempdb", "test.db")
}

func TestForEach(t *testing.T) {
	db, err := New("/test")
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginReadWriteTx()
	if err != nil {
		t.Fatal(err)
	}

	bkt, err := tx.CreateTopLevelBucket([]byte("top bucket"))
	if err != nil {
		t.Fatal(err)
	}

	nBkt, err := bkt.CreateBucket([]byte("nested bucket"))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		key := strconv.Itoa(i) + "bkt"
		nBkt.Put([]byte(key), []byte(key+"'s value"))
	}
}
