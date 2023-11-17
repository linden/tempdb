package tempdb

import (
	"testing"

	"github.com/btcsuite/btcwallet/walletdb/walletdbtest"
)

func TestInterface(t *testing.T) {
	walletdbtest.TestInterface(t, "tempdb", "test.db")
}
