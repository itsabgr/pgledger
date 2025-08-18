package ledger

import (
	"errors"
)

var ErrInsufficientBalance = errors.New("ledger: insufficient balance")

var ErrExists = errors.New("ledger: exists")
