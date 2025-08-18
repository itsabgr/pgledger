package ledger

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Ledger struct {
	pgx         *pgxpool.Pool
	callTimeout time.Duration
}

func New(pgx *pgxpool.Pool, callTimeout time.Duration) *Ledger {
	return &Ledger{pgx, callTimeout}
}

func (ledger *Ledger) Transfer(ctx context.Context, uid string, src, dst int64, val *big.Int, min *big.Int) (err error) {
	if val == nil ||
		val.Sign() <= 0 ||
		min == nil ||
		min.Sign() <= 0 ||
		src < 0 ||
		dst <= 0 ||
		len(uid) <= 0 ||
		len(uid) > 120 ||
		strings.TrimSpace(uid) != uid ||
		ctx == nil {
		panic(errors.New("ledger: invalid transfer arguments"))
	}
	var result int64
	err = ledger.call(ctx, pgx.ReadWrite, "SELECT func_transfer($1,$2,$3,$4,$5);", &result, uid, src, dst, val.Text(10), min.Text(10))
	if err != nil {
		return err
	}
	if result <= 0 {
		return ErrorCode{result}
	}
	return nil
}

func (ledger *Ledger) Balance(ctx context.Context, account int64) (val *big.Int, err error) {
	if account <= 0 || ctx == nil {
		panic(errors.New("ledger: invalid balance arguments"))
	}
	var valStr string
	err = ledger.call(ctx, pgx.ReadWrite, "SELECT func_balance($1);", &valStr, account)
	if err != nil {
		return nil, err
	}
	var ok bool
	val, ok = (&big.Int{}).SetString(valStr, 10)
	if !ok {
		return nil, errors.New("invalid big int")
	}
	return val, nil
}

func (ledger *Ledger) Exists(ctx context.Context, uid string) (exists bool, err error) {
	if len(uid) <= 0 ||
		len(uid) > 120 ||
		strings.TrimSpace(uid) != uid ||
		ctx == nil {
		panic(errors.New("ledger: invalid exists arguments"))
	}
	err = ledger.call(ctx, pgx.ReadOnly, "SELECT func_exists($1);", &exists, uid)
	return exists, err
}

func (ledger *Ledger) Ping(ctx context.Context) error {
	return ledger.pgx.Ping(ctx)
}

func (ledger *Ledger) Close(ctx context.Context) error {
	return ledger.Close(ctx)
}

func (ledger *Ledger) call(ctx context.Context, accessMode pgx.TxAccessMode, query string, result any, args ...any) (err error) {
	var tx pgx.Tx
	tx, err = ledger.pgx.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: accessMode,
	})
	if err != nil {
		return
	}

	rollback := true
	defer func() {
		if rollback {
			err = errors.Join(err, tx.Rollback(context.Background()))
		}
	}()

	timeout, cancelTimeout := context.WithTimeout(ctx, ledger.callTimeout)
	defer cancelTimeout()

	err = tx.QueryRow(timeout, query, args...).Scan(result)
	if err != nil {
		return
	}

	if accessMode == pgx.ReadWrite {
		rollback = false
		err = tx.Commit(timeout)
	}

	return
}
