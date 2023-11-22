package tempdb

import (
	"log/slog"
	"os"
	"testing"

	"github.com/btcsuite/btcwallet/walletdb/walletdbtest"
)

func TestMain(m *testing.M) {
	// create a new logger.
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			// hide the timestamp and level key.
			if attr.Key == slog.TimeKey || attr.Key == slog.LevelKey {
				return slog.Attr{}
			}

			return attr
		},
	})
	Logger = slog.New(h)

	// run the tests and forward the exit code.
	os.Exit(m.Run())
}

func TestInterface(t *testing.T) {
	Logger = slog.Default()

	walletdbtest.TestInterface(t, "tempdb", "test.db")
}
