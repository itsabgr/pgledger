package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

func Transfer(ctx context.Context, conn any, uid string, src, dst int64, val *big.Int, min *big.Int) (err error) {
	if val == nil ||
		val.Sign() <= 0 ||
		min == nil ||
		min.Sign() <= 0 ||
		src < 0 ||
		dst < 0 ||
		len(uid) <= 0 ||
		len(uid) > 120 ||
		strings.TrimSpace(uid) != uid ||
		ctx == nil {
		panic(errors.New("ledger: invalid transfer arguments"))
	}
	var result int64
	err = call(ctx, conn, "SELECT func_transfer($1,$2,$3,$4,$5);", &result, []any{uid, src, dst, val.Text(10), min.Text(10)})
	if err != nil {
		return err
	}
	if result > 0 {
		return nil
	}
	switch result {
	case -1, -3:
		return ErrExists
	case -2:
		return ErrInsufficientBalance
	default:
		return fmt.Errorf("ledger: invalid result %d", result)
	}
}

func Balance(ctx context.Context, conn any, account int64) (val *big.Int, err error) {
	if account <= 0 || ctx == nil {
		panic(errors.New("ledger: invalid balance arguments"))
	}
	var valStr string
	err = call(ctx, conn, "SELECT func_balance($1);", &valStr, []any{account})
	if err != nil {
		return nil, err
	}
	var ok bool
	val, ok = (&big.Int{}).SetString(valStr, 10)
	if !ok {
		return nil, errors.New("ledger: invalid numeric value")
	}
	return val, nil
}

func Exists(ctx context.Context, conn any, uid string) (exists bool, err error) {
	if len(uid) <= 0 ||
		len(uid) > 120 ||
		strings.TrimSpace(uid) != uid ||
		ctx == nil {
		panic(errors.New("ledger: invalid exists arguments"))
	}
	err = call(ctx, conn, "SELECT func_exists($1);", &exists, []any{uid})
	return exists, err
}

func call(ctx context.Context, conn any, cmd string, result any, args []any) error {

	switch db := conn.(type) {
	case interface {
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}:
		return db.QueryRowContext(ctx, cmd, args...).Scan(result)

	case interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}:
		rows, err := db.QueryContext(ctx, cmd, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		if !rows.Next() {
			return sql.ErrNoRows
		}

		return rows.Scan(result)

	default:
		panic(fmt.Errorf("ledger: invalid sql conn interface %T", conn))
	}

}
