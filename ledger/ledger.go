package ledger

import (
	"context"
	"errors"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Ledger struct {
	pgx *pgxpool.Pool
}

func New(pgx *pgxpool.Pool) *Ledger {
	return &Ledger{pgx}
}

func (ledger *Ledger) Transfer(ctx context.Context, uid string, src, dst int64, val *big.Int, min *big.Int) (err error) {
	var result int64
	err = ledger.call(ctx, pgx.ReadWrite, "SELECT func_transfer($1,$2,$3,$4,$5);", &result, uid, src, dst, val.String(), min.String())
	if err != nil {
		return err
	}
	if result <= 0 {
		return ErrorCode{result}
	}
	return nil
}

func (ledger *Ledger) Balance(ctx context.Context, account int64) (val *big.Int, err error) {
	var valStr string
	err = ledger.call(ctx, pgx.ReadWrite, "SELECT func_balance($1);", &valStr, account)
	if err != nil {
		return nil, err
	}
	val, ok := (&big.Int{}).SetString(valStr, 10)
	if !ok {
		return nil, errors.New("invalid big int")
	}
	return val, nil
}

func (ledger *Ledger) Exists(ctx context.Context, uid string) (exists bool, err error) {
	err = ledger.call(ctx, pgx.ReadOnly, "SELECT func_exists($1);", &exists, uid)
	return exists, err
}

func (ledger *Ledger) Close(ctx context.Context) error {
	return ledger.Close(ctx)
}

func (ledger *Ledger) call(ctx context.Context, accessMode pgx.TxAccessMode, query string, result any, args ...any) error {
	tx, err := ledger.pgx.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: accessMode,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, query, args...).Scan(result)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
