package tempdb

import (
	"errors"
	"log/slog"
	"io"

	"github.com/btcsuite/btcwallet/walletdb"
)

var ErrUnimplemented = errors.New("unimplemented")

// Ensure `DB` complies with the `walletdb.DB` interface.
var _ walletdb.DB = (*DB)(nil)

var Logger *slog.Logger

type DB struct {
	path  string
	state *State
}

func (db *DB) BeginReadTx() (walletdb.ReadTx, error) {
	Logger.Debug("beginning read transaction")

	return newTransaction(db.state), nil
}

func (db *DB) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	Logger.Debug("beginning read/write transaction")

	return newTransaction(db.state), nil
}

func (db *DB) View(f func(tx walletdb.ReadTx) error, reset func()) error {
	reset()

	tx, err := db.BeginReadTx()
	if err != nil {
		return err
	}

	return f(tx)
}

func (db *DB) Update(fn func(tx walletdb.ReadWriteTx) error, reset func()) error {
	reset()

	tx, err := db.BeginReadWriteTx()
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		return err
	}

	// TempDB Transaction
	ttx, ok := tx.(*Transaction)
	if !ok {
		return errors.New("transaction is not a tempdb transaction")
	}

	if ttx.rollback {
		return nil
	}

	return tx.Commit()
}

func (sl *DB) Close() error {
	return nil
}

// unimplemented.
func (sl *DB) Copy(w io.Writer) error {
	return ErrUnimplemented
}

// unimplemented.
func (sl *DB) PrintStats() string {
	return ErrUnimplemented.Error()
}

func New(args ...any) (walletdb.DB, error) {
	if Logger == nil {
		h := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level:slog.LevelError})
		Logger = slog.New(h)
	}

	if len(args) < 1 {
		return nil, errors.New("path argument is required")
	}

	path, ok := args[0].(string)
	if !ok {
		return nil, errors.New("path argument is not a string")
	}

	return &DB{
		path:  path,
		state: &State{},
	}, nil
}

func init() {
	err := walletdb.RegisterDriver(walletdb.Driver{
		DbType: "tempdb",

		Create: New,
		Open:   New,
	})

	if err != nil {
		panic(err)
	}
}
