package tempdb

import (
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/btcsuite/btcwallet/walletdb"
)

var ErrUnimplemented = errors.New("unimplemented")

// Ensure `DB` complies with the `walletdb.DB` interface.
var _ walletdb.DB = (*DB)(nil)

var Logger *slog.Logger

type DB struct {
	Path  string
	State *State
	lock  sync.Mutex
}

func (db *DB) BeginReadTx() (walletdb.ReadTx, error) {
	Logger.Debug("begin read transaction")

	return newTransaction(db.State, nil), nil
}

func (db *DB) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	Logger.Debug("begin read/write transaction")

	return newTransaction(db.State, &db.lock), nil
}

func (db *DB) View(f func(tx walletdb.ReadTx) error, reset func()) error {
	Logger.Debug("new view")

	reset()

	tx, err := db.BeginReadTx()
	if err != nil {
		return err
	}

	return f(tx)
}

func (db *DB) Update(fn func(tx walletdb.ReadWriteTx) error, reset func()) error {
	Logger.Debug("new update")

	reset()

	tx, err := db.BeginReadWriteTx()
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	// cast to a TempDB transaction.
	ttx := tx.(*Transaction)
	if ttx.Rolledback {
		return nil
	}

	return tx.Commit()
}

func (db *DB) Batch(fn func(tx walletdb.ReadWriteTx) error) error {
	Logger.Debug("new batch")

	return db.Update(fn, func() {})
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
		Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if len(args) < 1 {
		return nil, errors.New("path argument is required")
	}

	path, ok := args[0].(string)
	if !ok {
		return nil, errors.New("path argument is not a string")
	}

	return &DB{
		Path:  path,
		State: &State{},
	}, nil
}

// Open always returns `walletdb.ErrDbDoesNotExist` because tempdb is not persistent.
func Open(args ...any) (walletdb.DB, error) {
	return nil, walletdb.ErrDbDoesNotExist
}

func init() {
	err := walletdb.RegisterDriver(walletdb.Driver{
		DbType: "tempdb",

		Create: New,
		Open:   Open,
	})

	if err != nil {
		panic(err)
	}
}
