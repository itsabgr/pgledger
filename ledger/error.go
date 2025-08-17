package ledger

import (
	"strconv"
)

var _ error = ErrorCode{}

type ErrorCode struct {
	code int64
}

func (err ErrorCode) Error() string {
	return "ledger error: " + strconv.FormatInt(err.code, 10)
}

func (err ErrorCode) Code() int64 {
	return err.code
}

func (err ErrorCode) IsInsufficientBalance() bool {
	return err.code == -2
}

func (err ErrorCode) IsUIDExists() bool {
	switch err.code {
	case -1, -3:
		return true
	default:
		return false
	}
}
