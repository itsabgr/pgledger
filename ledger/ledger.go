package ledger

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"strings"
	"time"
)

type Ledger struct {
	pg          *sql.DB
	callTimeout time.Duration
}

func New(pg *sql.DB, callTimeout time.Duration) *Ledger {
	return &Ledger{pg, callTimeout}
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
	err = ledger.call(ctx, false, "SELECT func_transfer($1,$2,$3,$4,$5);", &result, uid, src, dst, val.Text(10), min.Text(10))
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
	err = ledger.call(ctx, false, "SELECT func_balance($1);", &valStr, account)
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
	err = ledger.call(ctx, true, "SELECT func_exists($1);", &exists, uid)
	return exists, err
}

func (ledger *Ledger) Ping(ctx context.Context) error {
	return ledger.pg.PingContext(ctx)
}

func (ledger *Ledger) Close(ctx context.Context) error {
	return ledger.pg.Close(ctx)
}
