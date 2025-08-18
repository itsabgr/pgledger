package ledger

import (
	"context"
	"database/sql"
	"errors"
)

func (ledger *Ledger) call(ctx context.Context, readonly bool, query string, result any, args ...any) (err error) {
	var tx *sql.Tx
	tx, err = ledger.pg.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  readonly,
	})
	if err != nil {
		return
	}

	rollback := true
	defer func() {
		if rollback {
			err = errors.Join(err, tx.Rollback())
		}
	}()

	timeout, cancelTimeout := context.WithTimeout(ctx, ledger.callTimeout)
	defer cancelTimeout()

	err = tx.QueryRowContext(timeout, query, args...).Scan(result)
	if err != nil {
		return
	}

	if readonly {
		return
	}

	rollback = false
	err = tx.Commit()

	return
}
